package com.lancedb.lancedb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MergeBuilder功能测试类
 */
public class MergeBuilderTest {
    
    @TempDir
    Path tempDir;
    
    private Connection connection;
    private Table table;
    
    @BeforeEach
    void setUp() {
        // 连接到临时数据库
        connection = Connection.connect(tempDir.toString());
        
        // 创建测试表
        List<Map<String, Object>> initialData = Arrays.asList(
            Map.of("id", 1, "name", "Alice", "age", 25, "last_update", "2024-01-01"),
            Map.of("id", 2, "name", "Bob", "age", 30, "last_update", "2024-01-01"),
            Map.of("id", 3, "name", "Charlie", "age", 35, "last_update", "2024-01-01")
        );
        
        table = connection.createTable("test_users", initialData, CreateTableMode.CREATE);
    }
    
    @AfterEach
    void tearDown() {
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            try {
                connection.dropTable("test_users");
            } catch (Exception e) {
                // 忽略清理错误
            }
        }
    }
    
    @Test
    void testBasicMergeInsert() {
        // 准备新数据：更新现有记录和插入新记录
        List<Map<String, Object>> newData = Arrays.asList(
            Map.of("id", 1, "name", "Alice Smith", "age", 26, "last_update", "2024-01-15"),  // 更新
            Map.of("id", 4, "name", "David", "age", 40, "last_update", "2024-01-15")         // 插入
        );
        
        // 执行merge操作
        MergeResult result = table.mergeInsert("id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute(newData);
        
        // 验证结果
        assertEquals(1, result.getNumUpdatedRows(), "应该有1行被更新");
        assertEquals(1, result.getNumInsertedRows(), "应该有1行被插入");
        assertEquals(0, result.getNumDeletedRows(), "应该没有行被删除");
        assertEquals(2, result.getTotalAffectedRows(), "总共应该有2行受影响");
        assertTrue(result.getVersion() > 0, "版本号应该大于0");
    }
    
    @Test
    void testConditionalUpdate() {
        // 准备新数据，但last_update比现有数据旧
        List<Map<String, Object>> newData = Arrays.asList(
            Map.of("id", 1, "name", "Alice Old", "age", 25, "last_update", "2023-12-01")  // 更旧的更新时间
        );
        
        // 执行merge操作，只有当目标行的last_update小于源行的last_update时才更新
        MergeResult result = table.mergeInsert("id")
            .whenMatchedUpdateAll(Optional.of("target.last_update < source.last_update"))
            .whenNotMatchedInsertAll()
            .execute(newData);
        
        // 验证结果：由于条件不满足，应该没有更新
        assertEquals(0, result.getNumUpdatedRows(), "由于条件不满足，应该没有行被更新");
        assertEquals(0, result.getNumInsertedRows(), "应该没有新行被插入");
        assertEquals(0, result.getNumDeletedRows(), "应该没有行被删除");
    }
    
    @Test
    void testConditionalDelete() {
        // 准备新数据，只包含部分现有记录
        List<Map<String, Object>> newData = Arrays.asList(
            Map.of("id", 1, "name", "Alice Updated", "age", 26, "last_update", "2024-01-15")
        );
        
        // 执行merge操作，删除不在源数据中的目标行，但只删除age大于30的行
        MergeResult result = table.mergeInsert("id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete(Optional.of("target.age > 30"))
            .execute(newData);
        
        // 验证结果：应该删除id=2和id=3的记录（age都大于30）
        assertEquals(1, result.getNumUpdatedRows(), "应该有1行被更新");
        assertEquals(0, result.getNumInsertedRows(), "应该没有新行被插入");
        assertEquals(2, result.getNumDeletedRows(), "应该有2行被删除（id=2和id=3，age都大于30）");
    }
    
    @Test
    void testTimeout() {
        // 准备大量数据来测试超时功能
        List<Map<String, Object>> newData = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            newData.add(Map.of("id", i, "name", "User" + i, "age", 20 + i, "last_update", "2024-01-15"));
        }
        
        // 执行merge操作，设置较短的超时时间
        MergeResult result = table.mergeInsert("id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .timeout(Duration.ofSeconds(5))  // 5秒超时
            .execute(newData);
        
        // 验证结果
        assertTrue(result.getTotalAffectedRows() > 0, "应该有行被处理");
        assertTrue(result.getVersion() > 0, "版本号应该大于0");
    }
    
    @Test
    void testMultipleColumns() {
        // 使用多个列作为匹配键
        List<Map<String, Object>> newData = Arrays.asList(
            Map.of("id", 1, "name", "Alice", "age", 26, "last_update", "2024-01-15")  // 更新现有记录
        );
        
        // 执行merge操作，使用id和name作为匹配键
        MergeResult result = table.mergeInsert("id", "name")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute(newData);
        
        // 验证结果
        assertEquals(1, result.getNumUpdatedRows(), "应该有1行被更新");
        assertEquals(0, result.getNumInsertedRows(), "应该没有新行被插入");
    }
    
    @Test
    void testEmptyData() {
        // 测试空数据的情况
        List<Map<String, Object>> newData = new ArrayList<>();
        
        // 执行merge操作
        MergeResult result = table.mergeInsert("id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute(newData);
        
        // 验证结果
        assertEquals(0, result.getNumUpdatedRows(), "应该没有行被更新");
        assertEquals(0, result.getNumInsertedRows(), "应该没有新行被插入");
        assertEquals(0, result.getNumDeletedRows(), "应该没有行被删除");
    }
    
    @Test
    void testMergeBuilderChaining() {
        // 测试方法链式调用
        MergeBuilder builder = table.mergeInsert("id")
            .whenMatchedUpdateAll(Optional.of("target.age < source.age"))
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete(Optional.of("target.age > 50"))
            .timeout(Duration.ofSeconds(30));
        
        // 验证构建器配置
        assertNotNull(builder, "构建器不应该为null");
        assertEquals("id", builder.getOnColumns()[0], "匹配列应该是'id'");
        assertEquals(table, builder.getTable(), "表引用应该正确");
        
        // 执行操作
        List<Map<String, Object>> newData = Arrays.asList(
            Map.of("id", 1, "name", "Alice", "age", 30, "last_update", "2024-01-15")  // age更大，应该更新
        );
        
        MergeResult result = builder.execute(newData);
        assertNotNull(result, "结果不应该为null");
    }
} 