package com.lancedb.lancedb;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/** Represents a LanceDB table with full CRUD operations. */
public class Table implements Closeable {
    private long nativeTableHandle;
    private String name;

    /** Get the table name */
    public String getName() {
        return name;
    }

    /** Get the table schema */
    public Schema getSchema() {
        return getSchemaNative();
    }

    /**
     * Add data to the table.
     *
     * @param data List of maps representing records to add
     */
    public void add(List<Map<String, Object>> data) {
        addNative(data);
    }

    /**
     * Add data from Arrow VectorSchemaRoot.
     *
     * @param data Arrow VectorSchemaRoot containing data
     */
    public void add(VectorSchemaRoot data) {
        addArrowNative(data);
    }

    /**
     * Delete rows matching the condition.
     *
     * @param condition SQL-like condition string
     * @return Number of rows deleted
     */
    public long delete(String condition) {
        return deleteNative(condition);
    }

    /**
     * Update rows matching the condition.
     *
     * @param condition SQL-like condition string
     * @param values Map of column names to new values
     * @return Number of rows updated
     */
    public long update(String condition, Map<String, Object> values) {
        return updateNative(condition, values);
    }

    /**
     * Count total rows in the table.
     *
     * @return Total number of rows
     */
    public long countRows() {
        return countRowsNative();
    }

    /**
     * Get table version.
     *
     * @return Current table version
     */
    public long getVersion() {
        return getVersionNative();
    }

    /**
     * Create a basic query builder for this table.
     *
     * @return QueryBuilder instance
     */
    public QueryBuilder query() {
        return new QueryBuilder(this);
    }

    /**
     * Create a vector search query.
     *
     * @param vector Query vector
     * @return VectorQueryBuilder instance
     */
    public VectorQueryBuilder search(float[] vector) {
        return new VectorQueryBuilder(this, vector);
    }

    // Native method declarations
    private native Schema getSchemaNative();
    private native void addNative(List<Map<String, Object>> data);
    private native void addArrowNative(VectorSchemaRoot data);
    private native long deleteNative(String condition);
    private native long updateNative(String condition, Map<String, Object> values);
    private native long countRowsNative();
    private native long getVersionNative();

    @Override
    public void close() {
        if (nativeTableHandle != 0) {
            releaseNativeTable(nativeTableHandle);
            nativeTableHandle = 0;
        }
    }

    private native void releaseNativeTable(long handle);

    // Package-private constructor for native instantiation
    Table() {}
}