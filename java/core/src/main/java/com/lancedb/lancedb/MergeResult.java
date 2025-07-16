package com.lancedb.lancedb;

/**
 * 表示merge insert操作的结果
 * 
 * 包含操作的版本信息和统计信息，如插入、更新和删除的行数。
 */
public class MergeResult {
    private final long version;
    private final long numInsertedRows;
    private final long numUpdatedRows;
    private final long numDeletedRows;

    /**
     * 包私有构造函数，由native代码调用
     * 
     * @param version 操作的提交版本
     * @param numInsertedRows 插入的行数
     * @param numUpdatedRows 更新的行数
     * @param numDeletedRows 删除的行数
     */
    MergeResult(long version, long numInsertedRows, long numUpdatedRows, long numDeletedRows) {
        this.version = version;
        this.numInsertedRows = numInsertedRows;
        this.numUpdatedRows = numUpdatedRows;
        this.numDeletedRows = numDeletedRows;
    }

    /**
     * 获取操作的提交版本
     * 
     * 版本为0表示与不返回提交版本的旧服务器兼容。
     * 
     * @return 操作的提交版本
     */
    public long getVersion() {
        return version;
    }

    /**
     * 获取插入的行数
     * 
     * @return 插入的行数
     */
    public long getNumInsertedRows() {
        return numInsertedRows;
    }

    /**
     * 获取更新的行数
     * 
     * @return 更新的行数
     */
    public long getNumUpdatedRows() {
        return numUpdatedRows;
    }

    /**
     * 获取删除的行数
     * 
     * 注意：这与内部对'deleted_rows'的引用不同，因为我们在技术上在处理过程中"删除"更新的行。
     * 但是这些行不会与用户共享。
     * 
     * @return 删除的行数
     */
    public long getNumDeletedRows() {
        return numDeletedRows;
    }

    /**
     * 获取操作的总影响行数
     * 
     * @return 插入、更新和删除的行数总和
     */
    public long getTotalAffectedRows() {
        return numInsertedRows + numUpdatedRows + numDeletedRows;
    }

    @Override
    public String toString() {
        return String.format(
            "MergeResult(version=%d, numInsertedRows=%d, numUpdatedRows=%d, numDeletedRows=%d)",
            version, numInsertedRows, numUpdatedRows, numDeletedRows
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        MergeResult that = (MergeResult) obj;
        return version == that.version &&
               numInsertedRows == that.numInsertedRows &&
               numUpdatedRows == that.numUpdatedRows &&
               numDeletedRows == that.numDeletedRows;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Long.hashCode(version);
        result = 31 * result + Long.hashCode(numInsertedRows);
        result = 31 * result + Long.hashCode(numUpdatedRows);
        result = 31 * result + Long.hashCode(numDeletedRows);
        return result;
    }
} 