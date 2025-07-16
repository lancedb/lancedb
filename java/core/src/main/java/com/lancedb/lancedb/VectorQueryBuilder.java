package com.lancedb.lancedb;

/** Vector search query builder extending base QueryBuilder. */
public class VectorQueryBuilder extends QueryBuilder {
    private float[] queryVector;
    private String distanceType = "l2";
    private String vectorColumn = "vector";

    protected VectorQueryBuilder(Table table, float[] vector) {
        super(table);
        this.queryVector = vector;
    }

    /**
     * Set the distance metric for vector search.
     *
     * @param distanceType Distance metric ("l2", "cosine", "dot")
     * @return This query builder
     */
    public VectorQueryBuilder distanceType(String distanceType) {
        this.distanceType = distanceType;
        return this;
    }

    /**
     * Set the vector column name.
     *
     * @param columnName Name of the vector column
     * @return This query builder
     */
    public VectorQueryBuilder vectorColumn(String columnName) {
        this.vectorColumn = columnName;
        return this;
    }

    @Override
    public VectorQueryBuilder limit(int limit) {
        super.limit(limit);
        return this;
    }

    @Override
    public VectorQueryBuilder offset(int offset) {
        super.offset(offset);
        return this;
    }

    @Override
    public VectorQueryBuilder where(String condition) {
        super.where(condition);
        return this;
    }

    @Override
    public VectorQueryBuilder select(String... columns) {
        super.select(columns);
        return this;
    }

    @Override
    public VectorQueryBuilder withRowId() {
        super.withRowId();
        return this;
    }

    // Override native methods to include vector search parameters
    private native java.util.List<java.util.Map<String, Object>> executeVectorQueryNative();
    private native org.apache.arrow.vector.VectorSchemaRoot executeVectorQueryArrowNative();
}