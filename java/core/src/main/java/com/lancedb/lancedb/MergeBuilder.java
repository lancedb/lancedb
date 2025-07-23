package com.lancedb.lancedb;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 用于配置和执行merge insert操作的构建器
 * 
 * Merge insert操作允许你根据指定的键列来合并新数据到现有表中。
 * 当新数据中的行与现有表中的行匹配时，可以选择更新现有行；
 * 当新数据中的行在现有表中不存在时，可以选择插入新行；
 * 当现有表中的行在新数据中不存在时，可以选择删除这些行。
 */
public class MergeBuilder {
    private long nativeMergeBuilderHandle;
    private final Table table;
    private final String[] onColumns;

    /**
     * 包私有构造函数，由Table.mergeInsert()方法调用
     * 
     * @param table 要执行merge操作的表
     * @param onColumns 用于匹配的列名数组
     */
    MergeBuilder(Table table, String[] onColumns) {
        this.table = table;
        this.onColumns = onColumns;
        this.nativeMergeBuilderHandle = createNativeMergeBuilder(table.getNativeHandle(), onColumns);
    }

    /**
     * 设置当行匹配时的更新行为
     * 
     * 当新数据中的行与现有表中的行匹配时，将用新数据中的行替换现有行。
     * 如果有多个匹配项，行为是未定义的。目前这会导致创建多个行副本，
     * 但该行为可能会发生变化。
     * 
     * @param condition 可选的SQL条件字符串。如果提供，只有满足条件的匹配行才会被更新。
     *                  任何不满足条件的行将保持原样。不满足条件不会导致"匹配行"变成"不匹配行"。
     *                  条件应该是一个SQL字符串。使用前缀target.来引用目标表（旧数据）中的行，
     *                  使用前缀source.来引用源表（新数据）中的行。
     *                  例如："target.last_update < source.last_update"
     * @return 当前MergeBuilder实例，支持链式调用
     */
    public MergeBuilder whenMatchedUpdateAll(Optional<String> condition) {
        whenMatchedUpdateAllNative(condition.orElse(null));
        return this;
    }

    /**
     * 设置当行匹配时的更新行为（无条件）
     * 
     * @return 当前MergeBuilder实例，支持链式调用
     */
    public MergeBuilder whenMatchedUpdateAll() {
        return whenMatchedUpdateAll(Optional.empty());
    }

    /**
     * 设置当行不匹配时的插入行为
     * 
     * 当新数据中的行在现有表中不存在时，这些行将被插入到目标表中。
     * 
     * @return 当前MergeBuilder实例，支持链式调用
     */
    public MergeBuilder whenNotMatchedInsertAll() {
        whenNotMatchedInsertAllNative();
        return this;
    }

    /**
     * 设置当源数据中不存在的目标行时的删除行为
     * 
     * 当现有表中的行在新数据中不存在时，这些行将被删除。
     * 
     * @param filter 可选的SQL过滤条件。如果为null，则所有此类行都将被删除。
     *               否则，条件将用作SQL过滤器来限制要删除的行。
     * @return 当前MergeBuilder实例，支持链式调用
     */
    public MergeBuilder whenNotMatchedBySourceDelete(Optional<String> filter) {
        whenNotMatchedBySourceDeleteNative(filter.orElse(null));
        return this;
    }

    /**
     * 设置当源数据中不存在的目标行时的删除行为（无条件）
     * 
     * @return 当前MergeBuilder实例，支持链式调用
     */
    public MergeBuilder whenNotMatchedBySourceDelete() {
        return whenNotMatchedBySourceDelete(Optional.empty());
    }

    /**
     * 设置操作的最大执行时间
     * 
     * 默认情况下，有一个30秒的超时，只在第一次尝试后强制执行。
     * 这是为了防止在解决冲突时花费太长时间重试。
     * 例如，如果写操作需要20秒并失败，第二次尝试将在10秒后被取消，
     * 达到30秒的超时。但是，第一次尝试成功需要一小时的写操作不会被取消。
     * 
     * 当设置此值时，超时将在所有尝试中强制执行，包括第一次。
     * 
     * @param timeout 最大执行时间
     * @return 当前MergeBuilder实例，支持链式调用
     */
    public MergeBuilder timeout(Duration timeout) {
        timeoutNative(timeout.toMillis());
        return this;
    }

    /**
     * 执行merge insert操作
     * 
     * @param newData 要合并的新数据，格式为List<Map<String, Object>>
     * @return MergeResult包含操作结果统计信息
     * @throws LanceDBException 如果操作失败
     */
    public MergeResult execute(List<Map<String, Object>> newData) {
        return executeNative(newData);
    }

    /**
     * 获取用于匹配的列名
     * 
     * @return 用于匹配的列名数组
     */
    public String[] getOnColumns() {
        return onColumns;
    }

    /**
     * 获取关联的表
     * 
     * @return 关联的表实例
     */
    public Table getTable() {
        return table;
    }

    /**
     * 释放本地资源
     */
    public void close() {
        if (nativeMergeBuilderHandle != 0) {
            releaseNativeMergeBuilder(nativeMergeBuilderHandle);
            nativeMergeBuilderHandle = 0;
        }
    }

    // Native方法声明
    private native long createNativeMergeBuilder(long tableHandle, String[] onColumns);
    private native void whenMatchedUpdateAllNative(String condition);
    private native void whenNotMatchedInsertAllNative();
    private native void whenNotMatchedBySourceDeleteNative(String filter);
    private native void timeoutNative(long timeoutMillis);
    private native MergeResult executeNative(List<Map<String, Object>> newData);
    private native void releaseNativeMergeBuilder(long handle);
} 