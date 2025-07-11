package com.lancedb.lancedb;

/** Base exception class for LanceDB operations. */
public class LanceDBException extends RuntimeException {
    public LanceDBException(String message) {
        super(message);
    }

    public LanceDBException(String message, Throwable cause) {
        super(message, cause);
    }
}

// java/core/src/main/java/com/lancedb/lancedb/TableNotFoundException.java
package com.lancedb.lancedb;

/** Exception thrown when a table is not found. */
public class TableNotFoundException extends LanceDBException {
    public TableNotFoundException(String tableName) {
        super("Table not found: " + tableName);
    }
}

// java/core/src/main/java/com/lancedb/lancedb/InvalidArgumentException.java
package com.lancedb.lancedb;

/** Exception thrown for invalid arguments. */
public class InvalidArgumentException extends LanceDBException {
    public InvalidArgumentException(String message) {
        super(message);
    }
}