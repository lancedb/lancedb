package com.lancedb.lancedb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ConnectionTest {
    @TempDir
    Path tempDir;

    private Connection connection;

    @BeforeEach
    void setUp() {
        connection = Connection.connect(tempDir.toString());
    }

    @AfterEach
    void tearDown() {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void testCreateTable() {
        List<Map<String, Object>> data = Arrays.asList(
                Map.of("vector", Arrays.asList(1.1f, 1.2f), "item", "foo", "price", 10.0),
                Map.of("vector", Arrays.asList(5.9f, 26.5f), "item", "bar", "price", 20.0)
        );

        Table table = connection.createTable("test_table", data);
        assertNotNull(table);
        assertEquals("test_table", table.getName());
    }

    @Test
    void testOpenTable() {
        // First create a table
        List<Map<String, Object>> data = Arrays.asList(
                Map.of("vector", Arrays.asList(1.1f, 1.2f), "item", "foo", "price", 10.0)
        );
        connection.createTable("test_table", data);

        // Then open it
        Table table = connection.openTable("test_table");
        assertNotNull(table);
        assertEquals("test_table", table.getName());
    }

    @Test
    void testTableNames() {
        // Create some tables
        List<Map<String, Object>> data = Arrays.asList(
                Map.of("vector", Arrays.asList(1.1f, 1.2f), "item", "foo", "price", 10.0)
        );

        connection.createTable("table1", data);
        connection.createTable("table2", data);

        List<String> tableNames = connection.tableNames();
        assertTrue(tableNames.contains("table1"));
        assertTrue(tableNames.contains("table2"));
    }

    @Test
    void testDropTable() {
        List<Map<String, Object>> data = Arrays.asList(
                Map.of("vector", Arrays.asList(1.1f, 1.2f), "item", "foo", "price", 10.0)
        );

        connection.createTable("test_table", data);
        assertTrue(connection.tableNames().contains("test_table"));

        connection.dropTable("test_table");
        assertFalse(connection.tableNames().contains("test_table"));
    }
}