package com.lancedb.lancedb;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Base query builder for LanceDB tables. */
public class QueryBuilder {
    protected Table table;
    protected int limit = -1;
    protected int offset = 0;
    protected String whereCondition;
    protected List<String> selectColumns = new ArrayList<>();
    protected boolean withRowId = false;

    protected QueryBuilder(Table table) {
        this.table = table;
    }

    /**
     * Set the maximum number of results to return.
     *
     * @param limit Maximum number of results
     * @return This query builder
     */
    public QueryBuilder limit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Set the number of results to skip.
     *
     * @param offset Number of results to skip
     * @return This query builder
     */
    public QueryBuilder offset(int offset) {
        this.offset = offset;
        return this;
    }

    /**
     * Add a WHERE condition.
     *
     * @param condition SQL-like condition string
     * @return This query builder
     */
    public QueryBuilder where(String condition) {
        this.whereCondition = condition;
        return this;
    }

    /**
     * Select specific columns.
     *
     * @param columns Column names to select
     * @return This query builder
     */
    public QueryBuilder select(String... columns) {
        this.selectColumns.clear();
        for (String column : columns) {
            this.selectColumns.add(column);
        }
        return this;
    }

    /**
     * Include row IDs in results.
     *
     * @return This query builder
     */
    public QueryBuilder withRowId() {
        this.withRowId = true;
        return this;
    }

    /**
     * Execute query and return results as list of maps.
     *
     * @return Query results
     */
    public List<Map<String, Object>> toList() {
        return executeQueryNative();
    }

    /**
     * Execute query and return results as Arrow VectorSchemaRoot.
     *
     * @return Query results as Arrow data
     */
    public VectorSchemaRoot toArrow() {
        return executeQueryArrowNative();
    }

    // Native method declarations
    private native List<Map<String, Object>> executeQueryNative();
    private native VectorSchemaRoot executeQueryArrowNative();
}