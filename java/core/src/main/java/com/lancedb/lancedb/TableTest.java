package com.lancedb.lancedb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class TableTest {
    @TempDir
    Path tempDir;

    private Connection connection;
    private Table table;

    @BeforeEach
    void setUp() {
        connection = Connection.connect(tempDir.toString());

        List<Map<String, Object>> data = Arrays.asList(
                Map.of("vector", Arrays.asList(1.1f, 1.2f), "item", "foo", "price", 10.0),
                Map.of("vector", Arrays.asList(5.9f, 26.5f), "item", "bar", "price", 20.0)
        );

        table = connection.createTable("test_table", data);
    }

    @AfterEach
    void tearDown() {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void testAddData() {
        List<Map<String, Object>> newData = Arrays.asList(
                Map.of("vector", Arrays.asList(2.1f, 2.2f), "item", "baz", "price", 30.0)
        );

        assertDoesNotThrow(() -> table.add(newData));

        // Verify the data was added by checking row count
        long rowCount = table.countRows();
        assertEquals(3, rowCount); // 2 initial + 1 added
    }

    @Test
    void testCountRows() {
        long rowCount = table.countRows();
        assertEquals(2, rowCount);
    }

    @Test
    void testDeleteRows() {
        long deletedCount = table.delete("item = 'foo'");
        assertEquals(1, deletedCount);

        long remainingRows = table.countRows();
        assertEquals(1, remainingRows);
    }

    @Test
    void testGetSchema() {
        assertDoesNotThrow(() -> {
            Schema schema = table.getSchema();
            assertNotNull(schema);
            // Verify schema contains expected fields
            assertTrue(schema.getFields().stream()
                    .anyMatch(field -> "vector".equals(field.getName())));
            assertTrue(schema.getFields().stream()
                    .anyMatch(field -> "item".equals(field.getName())));
            assertTrue(schema.getFields().stream()
                    .anyMatch(field -> "price".equals(field.getName())));
        });
    }

    @Test
    void testVectorSearch() {
        float[] queryVector = {1.0f, 1.0f};

        VectorQueryBuilder query = table.search(queryVector);
        assertNotNull(query);

        // Test chaining methods
        VectorQueryBuilder chainedQuery = query
                .limit(10)
                .distanceType("l2")
                .vectorColumn("vector");

        assertNotNull(chainedQuery);
    }
}