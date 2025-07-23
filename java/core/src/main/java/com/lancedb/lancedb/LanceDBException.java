package com.lancedb.lancedb;

public class LanceDBException extends Exception {
    public LanceDBException(String message) {
        super(message);
    }
    public LanceDBException(String message, Throwable cause) {
        super(message, cause);
    }
}