package com.lancedb.lancedb;

public enum CreateTableMode {
    CREATE("create"),
    OVERWRITE("overwrite"),
    EXIST_OK("exist_ok");

    private final String value;

    CreateTableMode(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}