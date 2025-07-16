package com.lancedb.lancedb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Merge Insert 集成测试
 * 
 * 模拟真实场景下的数据源调用，包括：
 * - 用户管理系统
 * - 订单管理系统  
 * - 库存管理系统
 * - 并发操作测试
 * - 大数据量测试
 */
@DisplayName("Merge Insert 集成测试")
public class MergeIntegrationTest {
    
    @TempDir
    Path tempDir;
    
    private Connection connection;
    private Table usersTable;
    private Table ordersTable;
    private Table inventoryTable;
    
    @BeforeEach
    void setUp() {
        connection = Connection.connect(tempDir.toString());
        setupTables();
    }
    
    @AfterEach
    void tearDown() {
        if (usersTable != null) usersTable.close();
        if (ordersTable != null) ordersTable.close();
        if (inventoryTable != null) inventoryTable.close();
        
        try {
            connection.dropTable("users");
            connection.dropTable("orders");
            connection.dropTable("inventory");
        } catch (Exception e) {
            // 忽略清理错误
        }
    }
    
    private void setupTables() {
        // 创建用户表
        List<Map<String, Object>> initialUsers = Arrays.asList(
            Map.of("user_id", 1, "username", "alice", "email", "alice@example.com", "status", "active", "created_at", "2024-01-01", "last_login", "2024-01-15"),
            Map.of("user_id", 2, "username", "bob", "email", "bob@example.com", "status", "active", "created_at", "2024-01-02", "last_login", "2024-01-14"),
            Map.of("user_id", 3, "username", "charlie", "email", "charlie@example.com", "status", "inactive", "created_at", "2024-01-03", "last_login", "2024-01-10")
        );
        usersTable = connection.createTable("users", initialUsers, CreateTableMode.CREATE);
        
        // 创建订单表
        List<Map<String, Object>> initialOrders = Arrays.asList(
            Map.of("order_id", 1001, "user_id", 1, "product_id", "P001", "quantity", 2, "total_amount", 199.99, "status", "pending", "created_at", "2024-01-15"),
            Map.of("order_id", 1002, "user_id", 2, "product_id", "P002", "quantity", 1, "total_amount", 89.99, "status", "shipped", "created_at", "2024-01-14"),
            Map.of("order_id", 1003, "user_id", 1, "product_id", "P003", "quantity", 3, "total_amount", 299.97, "status", "delivered", "created_at", "2024-01-13")
        );
        ordersTable = connection.createTable("orders", initialOrders, CreateTableMode.CREATE);
        
        // 创建库存表
        List<Map<String, Object>> initialInventory = Arrays.asList(
            Map.of("product_id", "P001", "product_name", "Laptop", "stock_quantity", 50, "price", 999.99, "category", "Electronics", "last_updated", "2024-01-15"),
            Map.of("product_id", "P002", "product_name", "Mouse", "stock_quantity", 100, "price", 29.99, "category", "Electronics", "last_updated", "2024-01-15"),
            Map.of("product_id", "P003", "product_name", "Keyboard", "stock_quantity", 75, "price", 99.99, "category", "Electronics", "last_updated", "2024-01-15")
        );
        inventoryTable = connection.createTable("inventory", initialInventory, CreateTableMode.CREATE);
    }
    
    @Nested
    @DisplayName("用户管理系统集成测试")
    class UserManagementIntegrationTest {
        
        @Test
        @DisplayName("用户信息批量更新 - 模拟用户管理系统")
        void testUserBulkUpdate() {
            // 模拟从用户管理系统获取的更新数据
            List<Map<String, Object>> userUpdates = Arrays.asList(
                Map.of("user_id", 1, "username", "alice_smith", "email", "alice.smith@example.com", "status", "active", "last_login", "2024-01-20"), // 更新现有用户
                Map.of("user_id", 4, "username", "david", "email", "david@example.com", "status", "active", "created_at", "2024-01-20", "last_login", "2024-01-20"), // 新用户
                Map.of("user_id", 5, "username", "eve", "email", "eve@example.com", "status", "pending", "created_at", "2024-01-20", "last_login", "2024-01-20")  // 新用户
            );
            
            // 执行merge操作 - 更新现有用户，插入新用户
            MergeResult result = usersTable.mergeInsert("user_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute(userUpdates);
            
            // 验证结果
            assertEquals(2, result.getNumInsertedRows(), "应该插入2个新用户");
            assertEquals(1, result.getNumUpdatedRows(), "应该更新1个现有用户");
            assertEquals(0, result.getNumDeletedRows(), "不应该删除任何用户");
            
            // 验证表状态
            assertEquals(5, usersTable.countRows(), "用户表应该有5行数据");
        }
        
        @Test
        @DisplayName("用户状态同步 - 模拟用户状态管理系统")
        void testUserStatusSync() {
            // 模拟用户状态管理系统的数据
            List<Map<String, Object>> statusUpdates = Arrays.asList(
                Map.of("user_id", 1, "status", "active", "last_login", "2024-01-20"),
                Map.of("user_id", 2, "status", "suspended", "last_login", "2024-01-19"),
                Map.of("user_id", 3, "status", "active", "last_login", "2024-01-20")
            );
            
            // 只更新状态信息，不插入新用户
            MergeResult result = usersTable.mergeInsert("user_id")
                .whenMatchedUpdateAll()
                .execute(statusUpdates);
            
            assertEquals(3, result.getNumUpdatedRows(), "应该更新3个用户的状态");
            assertEquals(0, result.getNumInsertedRows(), "不应该插入新用户");
        }
        
        @Test
        @DisplayName("用户清理 - 模拟用户清理系统")
        void testUserCleanup() {
            // 模拟活跃用户列表（只有部分用户）
            List<Map<String, Object>> activeUsers = Arrays.asList(
                Map.of("user_id", 1, "username", "alice", "status", "active"),
                Map.of("user_id", 2, "username", "bob", "status", "active")
            );
            
            // 删除不在活跃列表中的用户，但只删除inactive状态的用户
            MergeResult result = usersTable.mergeInsert("user_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedBySourceDelete(Optional.of("target.status = 'inactive'"))
                .execute(activeUsers);
            
            assertEquals(2, result.getNumUpdatedRows(), "应该更新2个活跃用户");
            assertEquals(1, result.getNumDeletedRows(), "应该删除1个inactive用户");
            
            // 验证表状态
            assertEquals(2, usersTable.countRows(), "应该只剩下2个活跃用户");
        }
    }
    
    @Nested
    @DisplayName("订单管理系统集成测试")
    class OrderManagementIntegrationTest {
        
        @Test
        @DisplayName("订单状态更新 - 模拟订单处理系统")
        void testOrderStatusUpdate() {
            // 模拟订单处理系统的状态更新
            List<Map<String, Object>> orderUpdates = Arrays.asList(
                Map.of("order_id", 1001, "status", "shipped", "shipped_at", "2024-01-20", "tracking_number", "TRK001"),
                Map.of("order_id", 1002, "status", "delivered", "delivered_at", "2024-01-20"),
                Map.of("order_id", 1004, "user_id", 3, "product_id", "P001", "quantity", 1, "total_amount", 999.99, "status", "pending", "created_at", "2024-01-20") // 新订单
            );
            
            // 条件更新：只有当订单状态发生变化时才更新
            MergeResult result = ordersTable.mergeInsert("order_id")
                .whenMatchedUpdateAll(Optional.of("target.status != source.status"))
                .whenNotMatchedInsertAll()
                .execute(orderUpdates);
            
            assertEquals(2, result.getNumUpdatedRows(), "应该更新2个订单状态");
            assertEquals(1, result.getNumInsertedRows(), "应该插入1个新订单");
        }
        
        @Test
        @DisplayName("订单取消处理 - 模拟订单取消系统")
        void testOrderCancellation() {
            // 模拟有效订单列表（排除已取消的订单）
            List<Map<String, Object>> validOrders = Arrays.asList(
                Map.of("order_id", 1001, "status", "pending"),
                Map.of("order_id", 1002, "status", "shipped")
            );
            
            // 删除不在有效订单列表中的订单，但只删除pending状态的订单
            MergeResult result = ordersTable.mergeInsert("order_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedBySourceDelete(Optional.of("target.status = 'pending'"))
                .execute(validOrders);
            
            assertEquals(2, result.getNumUpdatedRows(), "应该更新2个有效订单");
            assertEquals(1, result.getNumDeletedRows(), "应该删除1个pending订单");
        }
    }
    
    @Nested
    @DisplayName("库存管理系统集成测试")
    class InventoryManagementIntegrationTest {
        
        @Test
        @DisplayName("库存更新 - 模拟库存管理系统")
        void testInventoryUpdate() {
            // 模拟库存管理系统的更新数据
            List<Map<String, Object>> inventoryUpdates = Arrays.asList(
                Map.of("product_id", "P001", "stock_quantity", 45, "last_updated", "2024-01-20"), // 库存减少
                Map.of("product_id", "P002", "stock_quantity", 120, "last_updated", "2024-01-20"), // 库存增加
                Map.of("product_id", "P004", "product_name", "Monitor", "stock_quantity", 30, "price", 299.99, "category", "Electronics", "last_updated", "2024-01-20") // 新产品
            );
            
            // 条件更新：只有当库存发生变化时才更新
            MergeResult result = inventoryTable.mergeInsert("product_id")
                .whenMatchedUpdateAll(Optional.of("target.stock_quantity != source.stock_quantity"))
                .whenNotMatchedInsertAll()
                .execute(inventoryUpdates);
            
            assertEquals(2, result.getNumUpdatedRows(), "应该更新2个产品的库存");
            assertEquals(1, result.getNumInsertedRows(), "应该插入1个新产品");
        }
        
        @Test
        @DisplayName("库存预警 - 模拟库存预警系统")
        void testInventoryAlert() {
            // 模拟低库存产品列表
            List<Map<String, Object>> lowStockProducts = Arrays.asList(
                Map.of("product_id", "P001", "stock_quantity", 5, "alert_status", "low_stock"),
                Map.of("product_id", "P003", "stock_quantity", 10, "alert_status", "low_stock")
            );
            
            // 更新库存预警状态
            MergeResult result = inventoryTable.mergeInsert("product_id")
                .whenMatchedUpdateAll(Optional.of("target.stock_quantity <= 10"))
                .execute(lowStockProducts);
            
            assertEquals(2, result.getNumUpdatedRows(), "应该更新2个低库存产品的预警状态");
        }
    }
    
    @Nested
    @DisplayName("并发操作集成测试")
    class ConcurrencyIntegrationTest {
        
        @Test
        @DisplayName("并发用户更新 - 模拟多系统并发操作")
        void testConcurrentUserUpdates() throws InterruptedException {
            int threadCount = 5;
            int updatesPerThread = 10;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger totalUpdates = new AtomicInteger(0);
            
            // 创建多个线程同时进行用户更新
            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < updatesPerThread; j++) {
                            int userId = (threadId * updatesPerThread + j) % 10 + 1;
                            List<Map<String, Object>> userUpdate = Arrays.asList(
                                Map.of("user_id", userId, "username", "user" + userId, "last_login", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                            );
                            
                            MergeResult result = usersTable.mergeInsert("user_id")
                                .whenMatchedUpdateAll()
                                .whenNotMatchedInsertAll()
                                .timeout(Duration.ofSeconds(5))
                                .execute(userUpdate);
                            
                            totalUpdates.addAndGet((int) result.getTotalAffectedRows());
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            // 等待所有线程完成
            latch.await(30, TimeUnit.SECONDS);
            executor.shutdown();
            
            // 验证结果
            assertTrue(totalUpdates.get() > 0, "应该有更新操作发生");
            assertTrue(usersTable.countRows() >= 3, "用户表应该至少有初始的3行数据");
        }
        
        @Test
        @DisplayName("并发订单处理 - 模拟高并发订单系统")
        void testConcurrentOrderProcessing() throws InterruptedException {
            int threadCount = 3;
            int ordersPerThread = 20;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger totalOrders = new AtomicInteger(0);
            
            // 模拟多个订单处理线程
            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < ordersPerThread; j++) {
                            int orderId = 2000 + threadId * ordersPerThread + j;
                            List<Map<String, Object>> orderUpdate = Arrays.asList(
                                Map.of("order_id", orderId, "user_id", (orderId % 5) + 1, "product_id", "P00" + ((orderId % 3) + 1), 
                                       "quantity", (orderId % 5) + 1, "total_amount", (orderId % 100) + 10.0, 
                                       "status", "pending", "created_at", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                            );
                            
                            MergeResult result = ordersTable.mergeInsert("order_id")
                                .whenNotMatchedInsertAll()
                                .timeout(Duration.ofSeconds(3))
                                .execute(orderUpdate);
                            
                            totalOrders.addAndGet((int) result.getNumInsertedRows());
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            // 等待所有线程完成
            latch.await(60, TimeUnit.SECONDS);
            executor.shutdown();
            
            // 验证结果
            assertEquals(60, totalOrders.get(), "应该插入60个新订单");
            assertEquals(63, ordersTable.countRows(), "订单表应该有63行数据（3个初始 + 60个新订单）");
        }
    }
    
    @Nested
    @DisplayName("大数据量集成测试")
    class LargeDataIntegrationTest {
        
        @Test
        @DisplayName("大批量用户数据同步")
        void testLargeUserDataSync() {
            // 生成1000个用户数据
            List<Map<String, Object>> largeUserData = IntStream.range(1, 1001)
                .mapToObj(i -> {
                    Map<String, Object> user = new HashMap<>();
                    user.put("user_id", i);
                    user.put("username", "user" + i);
                    user.put("email", "user" + i + "@example.com");
                    user.put("status", i % 10 == 0 ? "inactive" : "active");
                    user.put("created_at", "2024-01-20");
                    user.put("last_login", LocalDateTime.now().minusDays(i % 30).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                    return user;
                })
                .collect(Collectors.toList());
            
            // 执行大批量merge操作
            long startTime = System.currentTimeMillis();
            MergeResult result = usersTable.mergeInsert("user_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .timeout(Duration.ofSeconds(30))
                .execute(largeUserData);
            long endTime = System.currentTimeMillis();
            
            // 验证结果
            assertEquals(997, result.getNumInsertedRows(), "应该插入997个新用户（1000 - 3个初始用户）");
            assertEquals(3, result.getNumUpdatedRows(), "应该更新3个现有用户");
            assertEquals(1000, usersTable.countRows(), "用户表应该有1000行数据");
            
            // 性能验证
            long duration = endTime - startTime;
            assertTrue(duration < 10000, "大批量操作应该在10秒内完成，实际耗时: " + duration + "ms");
            
            System.out.println("大批量用户数据同步完成:");
            System.out.println("- 插入用户数: " + result.getNumInsertedRows());
            System.out.println("- 更新用户数: " + result.getNumUpdatedRows());
            System.out.println("- 总耗时: " + duration + "ms");
            System.out.println("- 吞吐量: " + (1000.0 / duration * 1000) + " 用户/秒");
        }
        
        @Test
        @DisplayName("大批量订单数据更新")
        void testLargeOrderDataUpdate() {
            // 先生成1000个订单
            List<Map<String, Object>> initialOrders = IntStream.range(1, 1001)
                .mapToObj(i -> {
                    Map<String, Object> order = new HashMap<>();
                    order.put("order_id", 3000 + i);
                    order.put("user_id", (i % 10) + 1);
                    order.put("product_id", "P00" + ((i % 5) + 1));
                    order.put("quantity", (i % 10) + 1);
                    order.put("total_amount", (i % 1000) + 10.0);
                    order.put("status", "pending");
                    order.put("created_at", "2024-01-20");
                    return order;
                })
                .collect(Collectors.toList());
            
            ordersTable.add(new ArrayList<>(initialOrders));
            
            // 生成订单状态更新数据
            List<Map<String, Object>> orderUpdates = IntStream.range(1, 1001)
                .mapToObj(i -> {
                    Map<String, Object> update = new HashMap<>();
                    update.put("order_id", 3000 + i);
                    update.put("status", i % 3 == 0 ? "shipped" : (i % 3 == 1 ? "delivered" : "pending"));
                    update.put("updated_at", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                    return update;
                })
                .collect(Collectors.toList());
            
            // 执行大批量状态更新
            long startTime = System.currentTimeMillis();
            MergeResult result = ordersTable.mergeInsert("order_id")
                .whenMatchedUpdateAll(Optional.of("target.status != source.status"))
                .timeout(Duration.ofSeconds(30))
                .execute(orderUpdates);
            long endTime = System.currentTimeMillis();
            
            // 验证结果
            assertTrue(result.getNumUpdatedRows() > 0, "应该有订单被更新");
            assertEquals(1003, ordersTable.countRows(), "订单表应该有1003行数据（3个初始 + 1000个新订单）");
            
            // 性能验证
            long duration = endTime - startTime;
            assertTrue(duration < 10000, "大批量更新应该在10秒内完成，实际耗时: " + duration + "ms");
            
            System.out.println("大批量订单数据更新完成:");
            System.out.println("- 更新订单数: " + result.getNumUpdatedRows());
            System.out.println("- 总耗时: " + duration + "ms");
            System.out.println("- 吞吐量: " + (1000.0 / duration * 1000) + " 订单/秒");
        }
    }
    
    @Nested
    @DisplayName("错误处理集成测试")
    class ErrorHandlingIntegrationTest {
        
        @Test
        @DisplayName("超时处理测试")
        void testTimeoutHandling() {
            // 生成大量数据来测试超时
            List<Map<String, Object>> largeData = IntStream.range(1, 10001)
                .mapToObj(i -> {
                    Map<String, Object> user = new HashMap<>();
                    user.put("user_id", i);
                    user.put("username", "user" + i);
                    user.put("email", "user" + i + "@example.com");
                    user.put("status", "active");
                    user.put("created_at", "2024-01-20");
                    return user;
                })
                .collect(Collectors.toList());
            
            // 设置很短的超时时间来触发超时
            assertThrows(Exception.class, () -> {
                usersTable.mergeInsert("user_id")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .timeout(Duration.ofMillis(1)) // 1毫秒超时
                    .execute(largeData);
            }, "应该抛出超时异常");
        }
        
        @Test
        @DisplayName("空数据处理测试")
        void testEmptyDataHandling() {
            List<Map<String, Object>> emptyData = new ArrayList<>();
            
            MergeResult result = usersTable.mergeInsert("user_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute(emptyData);
            
            assertEquals(0, result.getTotalAffectedRows(), "空数据不应该产生任何影响");
        }
        
        @Test
        @DisplayName("无效条件处理测试")
        void testInvalidConditionHandling() {
            List<Map<String, Object>> userData = Arrays.asList(
                Map.of("user_id", 1, "username", "alice", "age", 25)
            );
            
            // 使用无效的SQL条件
            assertThrows(Exception.class, () -> {
                usersTable.mergeInsert("user_id")
                    .whenMatchedUpdateAll(Optional.of("invalid sql condition"))
                    .execute(userData);
            }, "无效的SQL条件应该抛出异常");
        }
    }
} 