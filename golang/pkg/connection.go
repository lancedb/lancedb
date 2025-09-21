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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
)

// Table represents a table in the LanceDB database
type Table struct {
	name       string
	connection *Connection
	handle     unsafe.Pointer
	mu         sync.RWMutex
	closed     bool
}

// Connection represents a connection to a LanceDB database
type Connection struct {
	handle unsafe.Pointer
	mu     sync.RWMutex
	closed bool
}

// ConnectionOptions holds options for establishing a database connection
type ConnectionOptions struct {
	// Simple implementation - these fields will be added as needed
	Region                  *string
	ReadConsistencyInterval *int
	StorageOptions          *StorageOptions
}

// StorageOptions holds storage-related options
type StorageOptions struct {
	// AWS S3 Options
	S3Config *S3Config `json:"s3_config,omitempty"`

	// Azure Blob Storage Options
	AzureConfig *AzureConfig `json:"azure_config,omitempty"`

	// Google Cloud Storage Options
	GCSConfig *GCSConfig `json:"gcs_config,omitempty"`

	// Local File System Options
	LocalConfig *LocalConfig `json:"local_config,omitempty"`

	// General Options
	BlockSize          *int    `json:"block_size,omitempty"`             // Block size in bytes
	MaxRetries         *int    `json:"max_retries,omitempty"`            // Maximum retry attempts
	RetryDelay         *int    `json:"retry_delay,omitempty"`            // Retry delay in milliseconds
	Timeout            *int    `json:"timeout,omitempty"`                // Request timeout in seconds
	AllowHTTP          *bool   `json:"allow_http,omitempty"`             // Allow HTTP connections (insecure)
	UserAgent          *string `json:"user_agent,omitempty"`             // Custom User-Agent header
	ConnectTimeout     *int    `json:"connect_timeout,omitempty"`        // Connection timeout in seconds
	ReadTimeout        *int    `json:"read_timeout,omitempty"`           // Read timeout in seconds
	PoolIdleTimeout    *int    `json:"pool_idle_timeout,omitempty"`      // Connection pool idle timeout
	PoolMaxIdlePerHost *int    `json:"pool_max_idle_per_host,omitempty"` // Max idle connections per host
}

// S3Config holds AWS S3 specific configuration
type S3Config struct {
	AccessKeyId       *string `json:"access_key_id,omitempty"`
	SecretAccessKey   *string `json:"secret_access_key,omitempty"`
	SessionToken      *string `json:"session_token,omitempty"`
	Region            *string `json:"region,omitempty"`
	Endpoint          *string `json:"endpoint,omitempty"`            // Custom endpoint (e.g., MinIO)
	ForcePathStyle    *bool   `json:"force_path_style,omitempty"`    // Use path-style addressing
	Profile           *string `json:"profile,omitempty"`             // AWS profile name
	AnonymousAccess   *bool   `json:"anonymous_access,omitempty"`    // Use anonymous access
	UseSSL            *bool   `json:"use_ssl,omitempty"`             // Use HTTPS
	ServerSideEncrypt *string `json:"server_side_encrypt,omitempty"` // Server-side encryption
	SSEKMSKeyId       *string `json:"sse_kms_key_id,omitempty"`      // KMS key ID for encryption
	StorageClass      *string `json:"storage_class,omitempty"`       // Storage class (STANDARD, IA, etc.)
}

// AzureConfig holds Azure Blob Storage specific configuration
type AzureConfig struct {
	AccountName  *string `json:"account_name,omitempty"`
	AccessKey    *string `json:"access_key,omitempty"`
	SasToken     *string `json:"sas_token,omitempty"`
	TenantId     *string `json:"tenant_id,omitempty"`
	ClientId     *string `json:"client_id,omitempty"`
	ClientSecret *string `json:"client_secret,omitempty"`
	Authority    *string `json:"authority,omitempty"`      // Authority URL
	Endpoint     *string `json:"endpoint,omitempty"`       // Custom endpoint
	UseHttps     *bool   `json:"use_https,omitempty"`      // Use HTTPS
	UseManagedId *bool   `json:"use_managed_id,omitempty"` // Use managed identity
}

// GCSConfig holds Google Cloud Storage specific configuration
type GCSConfig struct {
	ServiceAccountPath     *string `json:"service_account_path,omitempty"` // Path to service account JSON
	ServiceAccountKey      *string `json:"service_account_key,omitempty"`  // Service account JSON as string
	ProjectId              *string `json:"project_id,omitempty"`
	ApplicationCredentials *string `json:"application_credentials,omitempty"` // GOOGLE_APPLICATION_CREDENTIALS
	Endpoint               *string `json:"endpoint,omitempty"`                // Custom endpoint
	AnonymousAccess        *bool   `json:"anonymous_access,omitempty"`        // Use anonymous access
	UseSSL                 *bool   `json:"use_ssl,omitempty"`                 // Use HTTPS
}

// LocalConfig holds local file system specific configuration
type LocalConfig struct {
	CreateDirIfNotExists *bool `json:"create_dir_if_not_exists,omitempty"` // Create directory if it doesn't exist
	UseMemoryMap         *bool `json:"use_memory_map,omitempty"`           // Use memory mapping for files
	SyncWrites           *bool `json:"sync_writes,omitempty"`              // Sync writes to disk immediately
}

// Connect establishes a connection to a LanceDB database with context
func Connect(ctx context.Context, uri string, options *ConnectionOptions) (*Connection, error) {
	// Initialize the library
	C.simple_lancedb_init()

	cUri := C.CString(uri)
	defer C.free(unsafe.Pointer(cUri))

	var handle unsafe.Pointer
	var result *C.SimpleResult

	// Use storage options if provided
	if options != nil && options.StorageOptions != nil {
		// Serialize storage options to JSON
		optionsJSON, err := json.Marshal(options.StorageOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize storage options: %w", err)
		}

		cOptions := C.CString(string(optionsJSON))
		defer C.free(unsafe.Pointer(cOptions))

		result = C.simple_lancedb_connect_with_options(cUri, cOptions, &handle)
	} else {
		// Use basic connection without storage options
		result = C.simple_lancedb_connect(cUri, &handle)
	}

	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return nil, fmt.Errorf("failed to connect to LanceDB at %s: %s", uri, errorMsg)
		}
		return nil, fmt.Errorf("failed to connect to LanceDB at %s: unknown error", uri)
	}

	conn := &Connection{
		handle: handle,
		closed: false,
	}

	// Set finalizer to ensure cleanup
	runtime.SetFinalizer(conn, (*Connection).Close)

	return conn, nil
}

// Close closes the connection to the database
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || c.handle == nil {
		return nil
	}

	result := C.simple_lancedb_close(c.handle)
	defer C.simple_lancedb_result_free(result)

	c.handle = nil
	c.closed = true
	runtime.SetFinalizer(c, nil)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return fmt.Errorf("failed to close connection: %s", errorMsg)
		}
		return fmt.Errorf("failed to close connection: unknown error")
	}

	return nil
}

// TableNames returns a list of table names in the database with context
func (c *Connection) TableNames(ctx context.Context) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed || c.handle == nil {
		return nil, fmt.Errorf("connection is closed")
	}

	var cNames **C.char
	var count C.int

	result := C.simple_lancedb_table_names(c.handle, &cNames, &count)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return nil, fmt.Errorf("failed to get table names: %s", errorMsg)
		}
		return nil, fmt.Errorf("failed to get table names: unknown error")
	}

	if count == 0 {
		return []string{}, nil
	}

	// Convert C string array to Go string slice
	tableNames := make([]string, int(count))
	cNamesSlice := (*[1 << 20]*C.char)(unsafe.Pointer(cNames))[:count:count]
	for i, cName := range cNamesSlice {
		tableNames[i] = C.GoString(cName)
	}

	// Free the C memory
	C.simple_lancedb_free_table_names(cNames, count)

	return tableNames, nil
}

// OpenTable opens an existing table in the database with context
func (c *Connection) OpenTable(ctx context.Context, name string) (*Table, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed || c.handle == nil {
		return nil, fmt.Errorf("connection is closed")
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var tableHandle unsafe.Pointer
	result := C.simple_lancedb_open_table(c.handle, cName, &tableHandle)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return nil, fmt.Errorf("failed to open table %s: %s", name, errorMsg)
		}
		return nil, fmt.Errorf("failed to open table %s: unknown error", name)
	}

	table := &Table{
		name:       name,
		connection: c,
		handle:     tableHandle,
		closed:     false,
	}

	// Set finalizer to ensure cleanup
	runtime.SetFinalizer(table, (*Table).Close)

	return table, nil
}

// CreateTable creates a new table in the database with context
func (c *Connection) CreateTable(ctx context.Context, name string, schema Schema) (*Table, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed || c.handle == nil {
		return nil, fmt.Errorf("connection is closed")
	}

	// Convert schema to Arrow IPC bytes (more efficient than JSON)
	schemaIPC, err := c.schemaToIPC(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema to IPC: %w", err)
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	// Convert Go bytes to C pointers
	var cSchemaPtr *C.uchar
	if len(schemaIPC) > 0 {
		cSchemaPtr = (*C.uchar)(unsafe.Pointer(&schemaIPC[0]))
	}

	result := C.simple_lancedb_create_table_with_ipc(
		c.handle,
		cName,
		cSchemaPtr,
		C.size_t(len(schemaIPC)),
	)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return nil, fmt.Errorf("failed to create table %s: %s", name, errorMsg)
		}
		return nil, fmt.Errorf("failed to create table %s: unknown error", name)
	}

	// After successful creation, open the table to get a handle
	return c.OpenTable(ctx, name)
}

// DropTable drops a table from the database with context
func (c *Connection) DropTable(ctx context.Context, name string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed || c.handle == nil {
		return fmt.Errorf("connection is closed")
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	result := C.simple_lancedb_drop_table(c.handle, cName)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return fmt.Errorf("failed to drop table %s: %s", name, errorMsg)
		}
		return fmt.Errorf("failed to drop table %s: unknown error", name)
	}

	return nil
}

// schemaToIPC converts a Schema to Arrow IPC bytes for efficient transfer
func (c *Connection) schemaToIPC(schema Schema) ([]byte, error) {
	if schema.schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}

	// Create a temporary in-memory file using os.CreateTemp with os.DevNull pattern
	// This will work with the FileWriter which requires WriteSeeker interface
	tmpFile, err := os.CreateTemp("", "schema_*.arrow")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	// Use FileWriter to create a file format that can be read by FileReader in Rust
	writer, err := ipc.NewFileWriter(tmpFile, ipc.WithSchema(schema.schema))
	if err != nil {
		return nil, fmt.Errorf("failed to create IPC file writer: %w", err)
	}

	// Close the writer to finalize the file format (no data, just schema)
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close IPC writer: %w", err)
	}

	// Seek back to beginning and read the file contents
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to beginning: %w", err)
	}

	return io.ReadAll(tmpFile)
}

// schemaToJSON converts a Schema to JSON string for Rust bindings (deprecated in favor of schemaToIPC)
func (c *Connection) schemaToJSON(schema Schema) (string, error) {
	if schema.schema == nil {
		return "", fmt.Errorf("schema is nil")
	}

	fields := make([]map[string]interface{}, 0, schema.NumFields())

	for i := 0; i < schema.NumFields(); i++ {
		field, err := schema.Field(i)
		if err != nil {
			return "", fmt.Errorf("failed to get field %d: %w", i, err)
		}

		fieldMap := map[string]interface{}{
			"name":     field.Name,
			"nullable": field.Nullable,
		}

		// Convert Arrow DataType to string representation
		typeStr, err := c.arrowTypeToString(field.Type)
		if err != nil {
			return "", fmt.Errorf("failed to convert type for field %s: %w", field.Name, err)
		}
		fieldMap["type"] = typeStr

		fields = append(fields, fieldMap)
	}

	schemaMap := map[string]interface{}{
		"fields": fields,
	}

	jsonBytes, err := json.Marshal(schemaMap)
	if err != nil {
		return "", fmt.Errorf("failed to marshal schema to JSON: %w", err)
	}

	return string(jsonBytes), nil
}

// arrowTypeToString converts an Arrow DataType to string representation
func (c *Connection) arrowTypeToString(dataType arrow.DataType) (string, error) {
	switch dt := dataType.(type) {
	case *arrow.Int8Type:
		return "int8", nil
	case *arrow.Int16Type:
		return "int16", nil
	case *arrow.Int32Type:
		return "int32", nil
	case *arrow.Int64Type:
		return "int64", nil
	case *arrow.Float16Type:
		return "float16", nil
	case *arrow.Float32Type:
		return "float32", nil
	case *arrow.Float64Type:
		return "float64", nil
	case *arrow.StringType:
		return "string", nil
	case *arrow.BinaryType:
		return "binary", nil
	case *arrow.BooleanType:
		return "boolean", nil
	case *arrow.FixedSizeListType:
		// Handle vector types (fixed size list of floats)
		if dt.Elem().ID() == arrow.FLOAT16 {
			return fmt.Sprintf("fixed_size_list[float16;%d]", dt.Len()), nil
		}
		if dt.Elem().ID() == arrow.FLOAT32 {
			return fmt.Sprintf("fixed_size_list[float32;%d]", dt.Len()), nil
		}
		if dt.Elem().ID() == arrow.FLOAT64 {
			return fmt.Sprintf("fixed_size_list[float64;%d]", dt.Len()), nil
		}
		if dt.Elem().ID() == arrow.INT8 {
			return fmt.Sprintf("fixed_size_list[int8;%d]", dt.Len()), nil
		}
		if dt.Elem().ID() == arrow.INT16 {
			return fmt.Sprintf("fixed_size_list[int16;%d]", dt.Len()), nil
		}
		if dt.Elem().ID() == arrow.INT32 {
			return fmt.Sprintf("fixed_size_list[int32;%d]", dt.Len()), nil
		}
		if dt.Elem().ID() == arrow.INT64 {
			return fmt.Sprintf("fixed_size_list[int64;%d]", dt.Len()), nil
		}

		return "", fmt.Errorf("unsupported fixed size list element type: %v", dt.Elem())
	default:
		return "", fmt.Errorf("unsupported Arrow type: %v", dataType)
	}
}
