/*
 * SPDX-License-Identifier: Apache-2.0
 * SPDX-FileCopyrightText: Copyright The LanceDB Authors
 */

#ifndef LANCEDB_H
#define LANCEDB_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Opaque handle to a LanceDB ConnectBuilder
 */
typedef struct LanceDBConnectBuilder LanceDBConnectBuilder;

/**
 * Opaque handle to a LanceDB Connection
 */
typedef struct LanceDBConnection LanceDBConnection;

/**
 * Opaque handle to a LanceDB Table
 */
typedef struct LanceDBTable LanceDBTable;

/**
 * Opaque handle to a LanceDB Query
 */
typedef struct LanceDBQuery LanceDBQuery;

/**
 * Opaque handle to a LanceDB VectorQuery
 */
typedef struct LanceDBVectorQuery LanceDBVectorQuery;

/**
 * Opaque handle to a LanceDB QueryResult
 */
typedef struct LanceDBQueryResult LanceDBQueryResult;

/**
 * Opaque handle to Arrow RecordBatchReader
 */
typedef struct LanceDBRecordBatchReader LanceDBRecordBatchReader;

/**
 * Arrow C ABI Schema structure (opaque)
 */
typedef struct FFI_ArrowSchema FFI_ArrowSchema;

/**
 * Arrow C ABI Array structure (opaque)
 */
typedef struct FFI_ArrowArray FFI_ArrowArray;

/**
 * Error codes for LanceDB C API
 */
typedef enum {
    LANCEDB_SUCCESS = 0,
    LANCEDB_INVALID_ARGUMENT = 1,
    LANCEDB_INVALID_TABLE_NAME = 2,
    LANCEDB_INVALID_INPUT = 3,
    LANCEDB_TABLE_NOT_FOUND = 4,
    LANCEDB_DATABASE_NOT_FOUND = 5,
    LANCEDB_DATABASE_ALREADY_EXISTS = 6,
    LANCEDB_INDEX_NOT_FOUND = 7,
    LANCEDB_EMBEDDING_FUNCTION_NOT_FOUND = 8,
    LANCEDB_TABLE_ALREADY_EXISTS = 9,
    LANCEDB_CREATE_DIR = 10,
    LANCEDB_SCHEMA = 11,
    LANCEDB_RUNTIME = 12,
    LANCEDB_TIMEOUT = 13,
    LANCEDB_OBJECT_STORE = 14,
    LANCEDB_LANCE = 15,
    LANCEDB_HTTP = 16,
    LANCEDB_RETRY = 17,
    LANCEDB_ARROW = 18,
    LANCEDB_NOT_SUPPORTED = 19,
    LANCEDB_OTHER = 20,
    LANCEDB_UNKNOWN = 21
} LanceDBError;

/**
 * Error messages for LanceDB C API
 */
static const char* LANCEDB_ERROR_MESSAGES[] = {
    "Success",
    "Invalid argument",
    "Invalid table name",
    "Invalid input",
    "Table not found",
    "Database not found",
    "Database already exists",
    "Index not found",
    "Embedding function not found",
    "Table already exists",
    "Failed to create directory",
    "Schema error",
    "Runtime error",
    "Operation timed out",
    "Object store error",
    "Lance format error",
    "HTTP error",
    "Retryable error",
    "Arrow error",
    "Operation not supported",
    "Other error",
    "Unknown error"
};

/**
 * Convert error code to error message
 *
 * @param error - error code
 * @return Pointer to null-terminated C string containing the error message
 *
 * The returned string is valid for the lifetime of the program.
 * The caller must not free the returned string.
 */
static const char* lancedb_error_to_message(LanceDBError error) {
    if (error < 0 || error > LANCEDB_UNKNOWN) {
        return "Invalid error code";
    }
    return LANCEDB_ERROR_MESSAGES[error];
}

/**
 * Distance type enum for vector search
 */
typedef enum {
    LANCEDB_DISTANCE_L2 = 0,
    LANCEDB_DISTANCE_COSINE = 1,
    LANCEDB_DISTANCE_DOT = 2,
    LANCEDB_DISTANCE_HAMMING = 3
} LanceDBDistanceType;

/**
 * Index type enum
 */
typedef enum {
    LANCEDB_INDEX_AUTO = 0,
    LANCEDB_INDEX_BTREE = 1,
    LANCEDB_INDEX_BITMAP = 2,
    LANCEDB_INDEX_LABELLIST = 3,
    LANCEDB_INDEX_FTS = 4,
    LANCEDB_INDEX_IVF_FLAT = 5,
    LANCEDB_INDEX_IVF_PQ = 6,
    LANCEDB_INDEX_IVF_HNSW_PQ = 7,
    LANCEDB_INDEX_IVF_HNSW_SQ = 8
} LanceDBIndexType;

/**
 * Optimize type enum
 */
typedef enum {
    LANCEDB_OPTIMIZE_ALL = 0,     // Compact files and prune old versions
    LANCEDB_OPTIMIZE_COMPACT = 1, // Only compact files
    LANCEDB_OPTIMIZE_PRUNE = 2,   // Only prune old versions
    LANCEDB_OPTIMIZE_INDEX = 3    // Only rebuild indices
} LanceDBOptimizeType;

/**
 * Vector index configuration
 */
typedef struct {
    int num_partitions;         // Number of partitions for IVF indices (-1 = auto)
    int num_sub_vectors;        // Number of sub-vectors for PQ indices (-1 = auto)
    int max_iterations;         // Maximum training iterations (-1 = default)
    float sample_rate;          // Sampling rate for training (0.0 = default)
    LanceDBDistanceType distance_type; // Distance metric
    const char* accelerator;    // GPU accelerator ("cuda", "mps", or NULL for CPU)
    int replace;               // Replace existing index (1 = true, 0 = false)
} LanceDBVectorIndexConfig;

/**
 * Scalar index configuration
 */
typedef struct {
    int replace;                      // Replace existing index (1 = true, 0 = false)
    int force_update_statistics;      // Force update statistics (1 = true, 0 = false)
} LanceDBScalarIndexConfig;

/**
 * Full-text search index configuration
 */
typedef struct {
    const char* base_tokenizer;   // Base tokenizer ("simple", "whitespace", etc.)
    const char* language;         // Language for stemming ("en", "es", etc.)
    int max_tokens;              // Maximum tokens per document (-1 = no limit)
    int lowercase;               // Convert to lowercase (1 = true, 0 = false)
    int stem;                    // Apply stemming (1 = true, 0 = false)
    int remove_stop_words;       // Remove stop words (1 = true, 0 = false)
    int ascii_folding;           // Apply ASCII folding (1 = true, 0 = false)
    int replace;                 // Replace existing index (1 = true, 0 = false)
} LanceDBFtsIndexConfig;

/**
 * Merge insert configuration
 */
typedef struct {
    int when_matched_update_all;     // Update all columns for matched records (1 = true, 0 = false)
    int when_not_matched_insert_all; // Insert all new records (1 = true, 0 = false)
} LanceDBMergeInsertConfig;

/**
 * Create a ConnectBuilder for the given URI
 *
 * @param uri - null-terminated C string containing the database URI
 * @return Non-null pointer to LanceDBConnectBuilder on success, NULL on failure
 *
 * The URI can be:
 * - "/path/to/database" - local database on file system
 * - "s3://bucket/path/to/database" or "gs://bucket/path/to/database" - database on cloud object store
 * - "db://dbname" - LanceDB Cloud
 *
 * The returned pointer must be freed with lancedb_connect_builder_free().
 */
LanceDBConnectBuilder* lancedb_connect(const char* uri);

/**
 * Execute the connection and return a Connection handle
 *
 * @param builder - pointer to LanceDBConnectBuilder returned from lancedb_connect()
 * @return Non-null pointer to LanceDBConnection on success, NULL on failure
 *
 * The builder is consumed by this function and must not be used after calling.
 * The returned connection must be freed with lancedb_connection_free().
 */
LanceDBConnection* lancedb_connect_builder_execute(LanceDBConnectBuilder* builder);

/**
 * Free a ConnectBuilder
 *
 * @param builder - pointer to LanceDBConnectBuilder returned from lancedb_connect()
 *
 * After calling this function, the builder pointer must not be used.
 */
void lancedb_connect_builder_free(LanceDBConnectBuilder* builder);

/**
 * Get the URI of the connection
 *
 * @param connection - pointer to LanceDBConnection
 * @return Pointer to null-terminated C string containing the URI, or NULL on failure
 *
 * The returned string is valid until the connection is freed.
 * The caller must not free the returned string.
 */
const char* lancedb_connection_uri(const LanceDBConnection* connection);

/**
 * Get table names from the connection
 *
 * @param connection - pointer to LanceDBConnection
 * @param names_out - pointer to receive array of string pointers
 * @param count_out - pointer to receive count of table names
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * The caller is responsible for freeing the returned strings and array
 * using lancedb_free_table_names(). If error_message is provided and an error
 * occurs, the caller must free the error message with lancedb_free_string().
 */
LanceDBError lancedb_connection_table_names(
    const LanceDBConnection* connection,
    char*** names_out,
    size_t* count_out,
    char** error_message
);

/**
 * Free table names array returned by lancedb_connection_table_names
 *
 * @param names - array of string pointers returned by lancedb_connection_table_names
 * @param count - number of strings in the array
 */
void lancedb_free_table_names(char** names, size_t count);

/**
 * Open an existing table
 *
 * @param connection - pointer to LanceDBConnection
 * @param table_name - null-terminated C string containing the table name
 * @return Non-null pointer to LanceDBTable on success, NULL on failure
 *
 * The returned table must be freed with lancedb_table_free().
 */
LanceDBTable* lancedb_connection_open_table(
    const LanceDBConnection* connection,
    const char* table_name
);

/**
 * Drop a table from the database
 *
 * @param connection - pointer to LanceDBConnection
 * @param table_name - null-terminated C string containing the table name
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_connection_drop_table(
    const LanceDBConnection* connection,
    const char* table_name,
    char** error_message
);

/**
 * Rename a table in the database
 *
 * @param connection - pointer to LanceDBConnection
 * @param old_name - null-terminated C string containing the current table name
 * @param new_name - null-terminated C string containing the new table name
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * This operation is only supported in LanceDB Cloud.
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_connection_rename_table(
    const LanceDBConnection* connection,
    const char* old_name,
    const char* new_name,
    char** error_message
);

/**
 * Drop all tables in the database
 *
 * @param connection - pointer to LanceDBConnection
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_connection_drop_all_tables(
    const LanceDBConnection* connection,
    char** error_message
);

/**
 * Free a Connection
 *
 * @param connection - pointer to LanceDBConnection returned from lancedb_connect_builder_execute()
 *
 * After calling this function, the connection pointer must not be used.
 */
void lancedb_connection_free(LanceDBConnection* connection);

/**
 * Free a Table
 *
 * @param table - pointer to LanceDBTable returned from lancedb_connection_open_table()
 *
 * After calling this function, the table pointer must not be used.
 */
void lancedb_table_free(LanceDBTable* table);

/* ========== TABLE OPERATIONS ========== */

/**
 * Create a new table with Arrow schema
 *
 * @param connection - pointer to LanceDBConnection
 * @param table_name - null-terminated C string containing the table name
 * @param schema_ptr - pointer to Arrow C ABI schema
 * @param reader - pointer to LanceDBRecordBatchReader or NULL for empty table
 * @param table_out - pointer to receive the created table
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * The caller is responsible for freeing the returned table with lancedb_table_free().
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_table_create(
    const LanceDBConnection* connection,
    const char* table_name,
    const FFI_ArrowSchema* schema_ptr,
    LanceDBRecordBatchReader* reader,
    LanceDBTable** table_out,
    char** error_message
);

/**
 * Get table schema as Arrow C ABI
 *
 * @param table - pointer to LanceDBTable
 * @param schema_out - pointer to receive the Arrow schema
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *         Caller is responsible for releasing the schema using Arrow C ABI
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_table_arrow_schema(
    const LanceDBTable* table,
    FFI_ArrowSchema** schema_out,
    char** error_message
);

/**
 * Get table version
 *
 * @param table - pointer to LanceDBTable
 * @return Table version number on success, 0 on failure
 */
unsigned long long lancedb_table_version(const LanceDBTable* table);

/**
 * Count rows in table
 *
 * @param table - pointer to LanceDBTable
 * @return Number of rows in table on success, 0 on failure (or empty table)
 */
unsigned long long lancedb_table_count_rows(const LanceDBTable* table);

/**
 * Add data to table using Arrow RecordBatchReader
 *
 * @param table - pointer to LanceDBTable
 * @param reader - pointer to LanceDBRecordBatchReader
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * This function takes ownership of the reader and frees it automatically.
 * Do NOT call lancedb_record_batch_reader_free() after calling this function.
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_table_add(
    const LanceDBTable* table,
    LanceDBRecordBatchReader* reader,
    char** error_message
);

/**
 * Merge insert data into table (upsert operation)
 *
 * @param table - pointer to LanceDBTable
 * @param data - pointer to LanceDBRecordBatchReader containing data to merge
 * @param on_columns - array of column names to join on (typically key/id columns)
 * @param num_columns - number of columns in the on_columns array
 * @param config - pointer to LanceDBMergeInsertConfig for operation behavior (NULL for defaults)
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * This function performs an upsert operation by joining on the specified columns.
 * New records are inserted, existing records can be updated based on configuration.
 * This function takes ownership of the reader and frees it automatically.
 * Do NOT call lancedb_record_batch_reader_free() after calling this function.
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_table_merge_insert(
    const LanceDBTable* table,
    LanceDBRecordBatchReader* data,
    const char* const* on_columns,
    size_t num_columns,
    const LanceDBMergeInsertConfig* config,
    char** error_message
);

/**
 * Delete rows from table based on predicate
 *
 * @param table - pointer to LanceDBTable
 * @param predicate - null-terminated C string containing SQL WHERE clause
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_table_delete(
    const LanceDBTable* table,
    const char* predicate,
    char** error_message
);

/* ========== QUERY OPERATIONS ========== */

/**
 * Create a new query for the given table
 *
 * @param table - pointer to LanceDBTable
 * @return Pointer to LanceDBQuery on success, NULL on failure
 *         Caller must free with lancedb_query_free()
 */
LanceDBQuery* lancedb_query_new(const LanceDBTable* table);

/**
 * Create a vector query from table with query vector
 *
 * @param table - pointer to LanceDBTable
 * @param vector - array of floats representing the query vector
 * @param dimension - dimension of the vector
 * @return Pointer to LanceDBVectorQuery on success, NULL on failure
 *         Caller must free with lancedb_vector_query_free()
 */
LanceDBVectorQuery* lancedb_vector_query_new(
    const LanceDBTable* table,
    const float* vector,
    size_t dimension
);

/**
 * Set limit for query
 *
 * @param query - pointer to LanceDBQuery
 * @param limit - maximum number of results
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_query_limit(
    LanceDBQuery* query,
    size_t limit,
    char** error_message
);

/**
 * Set offset for query
 *
 * @param query - pointer to LanceDBQuery
 * @param offset - number of results to skip
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_query_offset(
    LanceDBQuery* query,
    size_t offset,
    char** error_message
);

/**
 * Set columns to select for query
 *
 * @param query - pointer to LanceDBQuery
 * @param columns - array of column name strings
 * @param num_columns - number of columns
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_query_select(
    LanceDBQuery* query,
    const char* const* columns,
    size_t num_columns,
    char** error_message
);

/**
 * Set WHERE filter for query
 *
 * @param query - pointer to LanceDBQuery
 * @param filter - SQL WHERE clause string
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_query_where_filter(
    LanceDBQuery* query,
    const char* filter,
    char** error_message
);

/**
 * Set limit for vector query
 *
 * @param query - pointer to LanceDBVectorQuery
 * @param limit - maximum number of results
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_vector_query_limit(
    LanceDBVectorQuery* query,
    size_t limit,
    char** error_message
);

/**
 * Set offset for vector query
 *
 * @param query - pointer to LanceDBVectorQuery
 * @param offset - number of results to skip
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_vector_query_offset(
    LanceDBVectorQuery* query,
    size_t offset,
    char** error_message
);

/**
 * Set vector column for vector query
 *
 * @param query - pointer to LanceDBVectorQuery
 * @param column - vector column name
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_vector_query_column(
    LanceDBVectorQuery* query,
    const char* column,
    char** error_message
);

/**
 * Set columns to select for vector query
 *
 * @param query - pointer to LanceDBVectorQuery
 * @param columns - array of column name strings
 * @param num_columns - number of columns
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_vector_query_select(
    LanceDBVectorQuery* query,
    const char* const* columns,
    size_t num_columns,
    char** error_message
);

/**
 * Set WHERE filter for vector query
 *
 * @param query - pointer to LanceDBVectorQuery
 * @param filter - SQL WHERE clause string
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_vector_query_where_filter(
    LanceDBVectorQuery* query,
    const char* filter,
    char** error_message
);

/**
 * Set distance type for vector query
 *
 * @param query - pointer to LanceDBVectorQuery
 * @param distance_type - distance metric to use
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_vector_query_distance_type(
    LanceDBVectorQuery* query,
    LanceDBDistanceType distance_type,
    char** error_message
);

/**
 * Set number of probes for vector query
 *
 * @param query - pointer to LanceDBVectorQuery
 * @param nprobes - number of probes for IVF indices
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_vector_query_nprobes(
    LanceDBVectorQuery* query,
    size_t nprobes,
    char** error_message
);

/**
 * Set refine factor for vector query
 *
 * @param query - pointer to LanceDBVectorQuery
 * @param refine_factor - refine factor for vector search
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_vector_query_refine_factor(
    LanceDBVectorQuery* query,
    unsigned int refine_factor,
    char** error_message
);

/**
 * Set ef parameter for HNSW vector query
 *
 * @param query - pointer to LanceDBVectorQuery
 * @param ef - ef parameter for HNSW search
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_vector_query_ef(
    LanceDBVectorQuery* query,
    size_t ef,
    char** error_message
);

/**
 * Execute query and return streaming result
 *
 * @param query - pointer to LanceDBQuery (consumed by this function)
 * @return Pointer to LanceDBQueryResult on success, NULL on failure
 *         Caller must free with lancedb_query_result_free()
 */
LanceDBQueryResult* lancedb_query_execute(LanceDBQuery* query);

/**
 * Execute vector query and return streaming result
 *
 * @param query - pointer to LanceDBVectorQuery (consumed by this function)
 * @return Pointer to LanceDBQueryResult on success, NULL on failure
 *         Caller must free with lancedb_query_result_free()
 */
LanceDBQueryResult* lancedb_vector_query_execute(LanceDBVectorQuery* query);

/**
 * Convert query result to Arrow RecordBatch arrays
 *
 * @param result - pointer to LanceDBQueryResult (consumed by this function)
 * @param result_arrays - pointer to receive array of Arrow C ABI arrays
 * @param result_schema - pointer to receive single Arrow C ABI schema
 * @param count_out - pointer to receive number of result batches
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *         Caller must free arrays with lancedb_free_arrow_arrays() and schema with lancedb_free_arrow_schema()
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_query_result_to_arrow(
    LanceDBQueryResult* result,
    struct FFI_ArrowArray*** result_arrays,
    struct FFI_ArrowSchema** result_schema,
    size_t* count_out,
    char** error_message
);

/**
 * Free a Query
 *
 * @param query - pointer to LanceDBQuery
 */
void lancedb_query_free(LanceDBQuery* query);

/**
 * Free a VectorQuery
 *
 * @param query - pointer to LanceDBVectorQuery
 */
void lancedb_vector_query_free(LanceDBVectorQuery* query);

/**
 * Free a QueryResult
 *
 * @param result - pointer to LanceDBQueryResult
 */
void lancedb_query_result_free(LanceDBQueryResult* result);

/* ========== VECTOR SEARCH OPERATIONS (SIMPLIFIED) ========== */

/**
 * Vector search using nearest_to with full result conversion
 *
 * @param table - pointer to LanceDBTable
 * @param vector - array of floats representing the query vector
 * @param dimension - dimension of the vector
 * @param limit - maximum number of results to return
 * @param column - vector column name (NULL for default "vector" column)
 * @param result_arrays - pointer to receive array of Arrow C ABI arrays
 * @param result_schema - pointer to receive single Arrow C ABI schema (shared by all arrays)
 * @param count_out - pointer to receive number of result batches
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *         Caller must free arrays with lancedb_free_arrow_arrays() and schema with lancedb_free_arrow_schema()
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_table_nearest_to(
    const LanceDBTable* table,
    const float* vector,
    size_t dimension,
    size_t limit,
    const char* column,
    struct FFI_ArrowArray*** result_arrays,
    struct FFI_ArrowSchema** result_schema,
    size_t* count_out,
    char** error_message
);

/* ========== UTILITY FUNCTIONS ========== */

/**
 * Create a RecordBatchReader from Arrow C ABI structures
 *
 * @param array - pointer to FFI_ArrowArray containing record batch data
 * @param schema - pointer to FFI_ArrowSchema containing the schema
 * @return Pointer to LanceDBRecordBatchReader on success, NULL on failure
 *
 * This function consumes the array according to Arrow C ABI specification.
 * The caller should NOT call the array's release function after passing it here.
 * The schema is only read and should still be released by the caller.
 * The caller is responsible for freeing the returned reader with
 * lancedb_record_batch_reader_free().
 */
LanceDBRecordBatchReader* lancedb_record_batch_reader_from_arrow(
    const struct FFI_ArrowArray* array,
    const struct FFI_ArrowSchema* schema
);

/**
 * Free RecordBatchReader
 *
 * @param reader - pointer to LanceDBRecordBatchReader
 */
void lancedb_record_batch_reader_free(LanceDBRecordBatchReader* reader);

/**
 * Free Arrow schema
 *
 * @param schema - pointer to Arrow schema
 */
void lancedb_free_arrow_schema(FFI_ArrowSchema* schema);

/* ========== INDEX OPERATIONS ========== */

/**
 * Create a vector index on table columns
 *
 * @param table - pointer to LanceDBTable
 * @param columns - array of null-terminated C strings containing column names
 * @param num_columns - number of columns in the array
 * @param index_type - type of vector index to create
 * @param config - pointer to LanceDBVectorIndexConfig or NULL for defaults
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_table_create_vector_index(
    const LanceDBTable* table,
    const char* const* columns,
    size_t num_columns,
    LanceDBIndexType index_type,
    const LanceDBVectorIndexConfig* config,
    char** error_message
);

/**
 * Create a scalar index on table columns
 *
 * @param table - pointer to LanceDBTable
 * @param columns - array of null-terminated C strings containing column names
 * @param num_columns - number of columns in the array
 * @param index_type - type of scalar index to create
 * @param config - pointer to LanceDBScalarIndexConfig or NULL for defaults
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_table_create_scalar_index(
    const LanceDBTable* table,
    const char* const* columns,
    size_t num_columns,
    LanceDBIndexType index_type,
    const LanceDBScalarIndexConfig* config,
    char** error_message
);

/**
 * Create a full-text search index on table columns
 *
 * @param table - pointer to LanceDBTable
 * @param columns - array of null-terminated C strings containing column names
 * @param num_columns - number of columns in the array
 * @param config - pointer to LanceDBFtsIndexConfig or NULL for defaults
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_table_create_fts_index(
    const LanceDBTable* table,
    const char* const* columns,
    size_t num_columns,
    const LanceDBFtsIndexConfig* config,
    char** error_message
);

/**
 * List all indices on the table
 *
 * @param table - pointer to LanceDBTable
 * @param indices_out - pointer to receive array of index info strings
 * @param count_out - pointer to receive the count of indices
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * The caller is responsible for freeing the returned strings and array
 * using lancedb_free_index_list(). If error_message is provided and an error
 * occurs, the caller must free the error message with lancedb_free_string().
 */
LanceDBError lancedb_table_list_indices(
    const LanceDBTable* table,
    char*** indices_out,
    size_t* count_out,
    char** error_message
);

/**
 * Drop an index
 *
 * @param table - pointer to LanceDBTable
 * @param index_name - null-terminated C string containing the index name
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_table_drop_index(
    const LanceDBTable* table,
    const char* index_name,
    char** error_message
);

/**
 * Optimize table (rebuild indices and compact files)
 *
 * @param table - pointer to LanceDBTable
 * @param optimize_type - type of optimization to perform
 * @param error_message - optional pointer to receive detailed error message (NULL to ignore)
 * @return Error code indicating success or failure
 *
 * If error_message is provided and an error occurs, the caller must free
 * the error message with lancedb_free_string().
 */
LanceDBError lancedb_table_optimize(
    const LanceDBTable* table,
    LanceDBOptimizeType optimize_type,
    char** error_message
);

/**
 * Free index list array returned by lancedb_table_list_indices
 *
 * @param indices - array of string pointers returned by lancedb_table_list_indices
 * @param count - number of strings in the array
 */
void lancedb_free_index_list(char** indices, size_t count);

/**
 * Free Arrow arrays returned by vector search functions
 *
 * @param arrays - array of Arrow C ABI array pointers
 * @param count - number of arrays
 */
void lancedb_free_arrow_arrays(
    struct FFI_ArrowArray** arrays,
    size_t count
);

/**
 * Free string returned by LanceDB functions
 *
 * @param str - string pointer returned by LanceDB functions
 */
void lancedb_free_string(char* str);

#ifdef __cplusplus
}
#endif

#endif /* LANCEDB_H */
