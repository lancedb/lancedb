package com.lancedb.lancedb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Merge Insert 性能测试
 * 
 * 测试不同场景下的性能表现：
 * - 小数据量测试（100-1000行）
 * - 中等数据量测试（1000-10000行）
 * - 大数据量测试（10000-100000行）
 * - 并发性能测试
 * - 内存使用测试
 */
@DisplayName("Merge Insert 性能测试")
public class MergePerformanceTest {
    
    @TempDir
    Path tempDir;
    
    private Connection connection;
    private Table testTable;
    
    @BeforeEach
    void setUp() {
        connection = Connection.connect(tempDir.toString());
        
        // 创建测试表，包含初始数据
        List<Map<String, Object>> initialData = IntStream.range(1, 101)
            .mapToObj(i -> {
                Map<String, Object> record = new HashMap<>();
                record.put("id", i);
                record.put("name", "user" + i);
                record.put("age", 20 + (i % 50));
                record.put("email", "user" + i + "@example.com");
                record.put("status", i % 10 == 0 ? "inactive" : "active");
                record.put("score", (double) (i % 100));
                record.put("created_at", "2024-01-01");
                record.put("updated_at", "2024-01-15");
                return record;
            })
            .collect(Collectors.toList());
        
        testTable = connection.createTable("performance_test", initialData, CreateTableMode.CREATE);
    }
    
    @AfterEach
    void tearDown() {
        if (testTable != null) {
            testTable.close();
        }
        try {
            connection.dropTable("performance_test");
        } catch (Exception e) {
            // 忽略清理错误
        }
    }
    
    @Test
    @DisplayName("小数据量性能测试 - 100行数据")
    void testSmallDataPerformance() {
        performMergePerformanceTest(100, "小数据量测试");
    }
    
    @Test
    @DisplayName("中等数据量性能测试 - 1000行数据")
    void testMediumDataPerformance() {
        performMergePerformanceTest(1000, "中等数据量测试");
    }
    
    @Test
    @DisplayName("大数据量性能测试 - 10000行数据")
    void testLargeDataPerformance() {
        performMergePerformanceTest(10000, "大数据量测试");
    }
    
    @Test
    @DisplayName("超大数据量性能测试 - 50000行数据")
    void testExtraLargeDataPerformance() {
        performMergePerformanceTest(50000, "超大数据量测试");
    }
    
    private void performMergePerformanceTest(int dataSize, String testName) {
        // 生成测试数据
        List<Map<String, Object>> testData = generateTestData(dataSize);
        
        // 记录内存使用情况
        Runtime runtime = Runtime.getRuntime();
        long memoryBefore = runtime.totalMemory() - runtime.freeMemory();
        
        // 执行性能测试
        long startTime = System.currentTimeMillis();
        MergeResult result = testTable.mergeInsert("id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .timeout(Duration.ofSeconds(60))
            .execute(testData);
        long endTime = System.currentTimeMillis();
        
        long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
        long memoryUsed = memoryAfter - memoryBefore;
        
        // 计算性能指标
        long duration = endTime - startTime;
        double throughput = (double) dataSize / duration * 1000; // 行/秒
        double memoryPerRecord = (double) memoryUsed / dataSize; // 字节/行
        
        // 输出性能报告
        System.out.println("\n=== " + testName + " 性能报告 ===");
        System.out.println("数据量: " + dataSize + " 行");
        System.out.println("执行时间: " + duration + " ms");
        System.out.println("吞吐量: " + String.format("%.2f", throughput) + " 行/秒");
        System.out.println("内存使用: " + memoryUsed + " 字节 (" + String.format("%.2f", memoryPerRecord) + " 字节/行)");
        System.out.println("插入行数: " + result.getNumInsertedRows());
        System.out.println("更新行数: " + result.getNumUpdatedRows());
        System.out.println("删除行数: " + result.getNumDeletedRows());
        System.out.println("总影响行数: " + result.getTotalAffectedRows());
        
        // 性能断言
        assertTrue(duration < 30000, "操作应该在30秒内完成，实际耗时: " + duration + "ms");
        assertTrue(throughput > 100, "吞吐量应该大于100行/秒，实际: " + throughput + " 行/秒");
        assertTrue(memoryPerRecord < 1000, "每行内存使用应该小于1000字节，实际: " + memoryPerRecord + " 字节/行");
        
        // 验证结果正确性
        assertEquals(dataSize, result.getTotalAffectedRows(), "总影响行数应该等于数据量");
        assertTrue(result.getVersion() > 0, "版本号应该大于0");
    }
    
    @Test
    @DisplayName("并发性能测试 - 多线程并发操作")
    void testConcurrentPerformance() throws InterruptedException {
        int threadCount = 4;
        int dataSizePerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicLong totalDuration = new AtomicLong(0);
        AtomicLong totalThroughput = new AtomicLong(0);
        
        // 记录开始时间
        long startTime = System.currentTimeMillis();
        
        // 创建多个线程并发执行merge操作
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    // 每个线程生成不同的数据
                    List<Map<String, Object>> threadData = generateTestDataForThread(dataSizePerThread, threadId);
                    
                    long threadStartTime = System.currentTimeMillis();
                    MergeResult result = testTable.mergeInsert("id")
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .timeout(Duration.ofSeconds(30))
                        .execute(threadData);
                    long threadEndTime = System.currentTimeMillis();
                    
                    long threadDuration = threadEndTime - threadStartTime;
                    double threadThroughput = (double) dataSizePerThread / threadDuration * 1000;
                    
                    totalDuration.addAndGet(threadDuration);
                    totalThroughput.addAndGet((long) threadThroughput);
                    
                    System.out.println("线程 " + threadId + " 完成: " + threadDuration + "ms, " + 
                                     String.format("%.2f", threadThroughput) + " 行/秒");
                    
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有线程完成
        latch.await(120, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();
        executor.shutdown();
        
        // 计算总体性能指标
        long totalTime = endTime - startTime;
        double avgDuration = (double) totalDuration.get() / threadCount;
        double avgThroughput = (double) totalThroughput.get() / threadCount;
        double totalThroughputOverall = (double) (threadCount * dataSizePerThread) / totalTime * 1000;
        
        System.out.println("\n=== 并发性能测试报告 ===");
        System.out.println("线程数: " + threadCount);
        System.out.println("每线程数据量: " + dataSizePerThread + " 行");
        System.out.println("总数据量: " + (threadCount * dataSizePerThread) + " 行");
        System.out.println("总执行时间: " + totalTime + " ms");
        System.out.println("平均线程执行时间: " + String.format("%.2f", avgDuration) + " ms");
        System.out.println("平均线程吞吐量: " + String.format("%.2f", avgThroughput) + " 行/秒");
        System.out.println("总体吞吐量: " + String.format("%.2f", totalThroughputOverall) + " 行/秒");
        
        // 性能断言
        assertTrue(totalTime < 60000, "并发操作应该在60秒内完成，实际耗时: " + totalTime + "ms");
        assertTrue(avgThroughput > 50, "平均线程吞吐量应该大于50行/秒，实际: " + avgThroughput + " 行/秒");
        assertTrue(totalThroughputOverall > 200, "总体吞吐量应该大于200行/秒，实际: " + totalThroughputOverall + " 行/秒");
    }
    
    @Test
    @DisplayName("条件更新性能测试")
    void testConditionalUpdatePerformance() {
        int dataSize = 5000;
        List<Map<String, Object>> testData = generateTestData(dataSize);
        
        // 测试无条件更新
        long startTime1 = System.currentTimeMillis();
        MergeResult result1 = testTable.mergeInsert("id")
            .whenMatchedUpdateAll()
            .execute(testData);
        long endTime1 = System.currentTimeMillis();
        long duration1 = endTime1 - startTime1;
        
        // 测试条件更新
        long startTime2 = System.currentTimeMillis();
        MergeResult result2 = testTable.mergeInsert("id")
            .whenMatchedUpdateAll(Optional.of("target.age < source.age"))
            .execute(testData);
        long endTime2 = System.currentTimeMillis();
        long duration2 = endTime2 - startTime2;
        
        System.out.println("\n=== 条件更新性能对比 ===");
        System.out.println("数据量: " + dataSize + " 行");
        System.out.println("无条件更新时间: " + duration1 + " ms");
        System.out.println("条件更新时间: " + duration2 + " ms");
        System.out.println("性能差异: " + String.format("%.2f", (double) duration2 / duration1) + "x");
        
        // 性能断言
        assertTrue(duration1 < 10000, "无条件更新应该在10秒内完成");
        assertTrue(duration2 < 15000, "条件更新应该在15秒内完成");
        assertTrue(duration2 <= duration1 * 2, "条件更新不应该比无条件更新慢超过2倍");
    }
    
    @Test
    @DisplayName("内存使用效率测试")
    void testMemoryEfficiency() {
        int dataSize = 10000;
        Runtime runtime = Runtime.getRuntime();
        
        // 强制垃圾回收
        System.gc();
        long memoryBefore = runtime.totalMemory() - runtime.freeMemory();
        
        // 执行多次操作测试内存泄漏
        for (int i = 0; i < 5; i++) {
            List<Map<String, Object>> testData = generateTestData(dataSize);
            
            MergeResult result = testTable.mergeInsert("id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute(testData);
            
            System.out.println("第 " + (i + 1) + " 次操作完成，影响行数: " + result.getTotalAffectedRows());
        }
        
        // 强制垃圾回收
        System.gc();
        long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
        long memoryIncrease = memoryAfter - memoryBefore;
        
        System.out.println("\n=== 内存使用效率测试 ===");
        System.out.println("初始内存使用: " + memoryBefore + " 字节");
        System.out.println("最终内存使用: " + memoryAfter + " 字节");
        System.out.println("内存增长: " + memoryIncrease + " 字节");
        System.out.println("平均每次操作内存增长: " + (memoryIncrease / 5) + " 字节");
        
        // 内存断言
        assertTrue(memoryIncrease < 100 * 1024 * 1024, "内存增长应该小于100MB，实际: " + memoryIncrease + " 字节");
        assertTrue(memoryIncrease / 5 < 20 * 1024 * 1024, "每次操作内存增长应该小于20MB");
    }
    
    @RepeatedTest(3)
    @DisplayName("重复性能测试 - 验证性能稳定性")
    void testPerformanceStability() {
        int dataSize = 2000;
        List<Map<String, Object>> testData = generateTestData(dataSize);
        
        long startTime = System.currentTimeMillis();
        MergeResult result = testTable.mergeInsert("id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute(testData);
        long endTime = System.currentTimeMillis();
        
        long duration = endTime - startTime;
        double throughput = (double) dataSize / duration * 1000;
        
        System.out.println("重复测试 - 执行时间: " + duration + "ms, 吞吐量: " + String.format("%.2f", throughput) + " 行/秒");
        
        // 性能稳定性断言
        assertTrue(duration < 10000, "操作应该在10秒内完成");
        assertTrue(throughput > 100, "吞吐量应该大于100行/秒");
    }
    
    @Test
    @DisplayName("超时性能测试")
    void testTimeoutPerformance() {
        int dataSize = 1000;
        List<Map<String, Object>> testData = generateTestData(dataSize);
        
        // 测试不同超时设置的性能
        Duration[] timeouts = {
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            Duration.ofSeconds(10)
        };
        
        for (Duration timeout : timeouts) {
            long startTime = System.currentTimeMillis();
            MergeResult result = testTable.mergeInsert("id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .timeout(timeout)
                .execute(testData);
            long endTime = System.currentTimeMillis();
            
            long duration = endTime - startTime;
            double throughput = (double) dataSize / duration * 1000;
            
            System.out.println("超时 " + timeout.getSeconds() + "s - 执行时间: " + duration + "ms, " +
                             "吞吐量: " + String.format("%.2f", throughput) + " 行/秒");
            
            // 验证超时设置不影响性能
            assertTrue(duration < timeout.toMillis(), "执行时间应该小于超时时间");
        }
    }
    
    // 辅助方法：生成测试数据
    private List<Map<String, Object>> generateTestData(int size) {
        return IntStream.range(1, size + 1)
            .mapToObj(i -> {
                Map<String, Object> record = new HashMap<>();
                record.put("id", i);
                record.put("name", "user" + i);
                record.put("age", 20 + (i % 50));
                record.put("email", "user" + i + "@example.com");
                record.put("status", i % 10 == 0 ? "inactive" : "active");
                record.put("score", (double) (i % 100));
                record.put("created_at", "2024-01-01");
                record.put("updated_at", "2024-01-20");
                return record;
            })
            .collect(Collectors.toList());
    }
    
    // 辅助方法：为线程生成测试数据
    private List<Map<String, Object>> generateTestDataForThread(int size, int threadId) {
        return IntStream.range(1, size + 1)
            .mapToObj(i -> {
                Map<String, Object> record = new HashMap<>();
                record.put("id", threadId * 10000 + i); // 确保不同线程的数据不冲突
                record.put("name", "thread" + threadId + "_user" + i);
                record.put("age", 20 + (i % 50));
                record.put("email", "thread" + threadId + "_user" + i + "@example.com");
                record.put("status", i % 10 == 0 ? "inactive" : "active");
                record.put("score", (double) (i % 100));
                record.put("created_at", "2024-01-01");
                record.put("updated_at", "2024-01-20");
                return record;
            })
            .collect(Collectors.toList());
    }
} 