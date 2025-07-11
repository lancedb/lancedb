package com.lancedb.lancedb;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ConnectionOptions {
    private Optional<String> apiKey = Optional.empty();
    private String region = "us-east-1";
    private Optional<String> hostOverride = Optional.empty();
    private Optional<Double> readConsistencyInterval = Optional.empty();
    private Map<String, String> storageOptions = new HashMap<>();

    public ConnectionOptions apiKey(String apiKey) {
        this.apiKey = Optional.of(apiKey);
        return this;
    }

    public ConnectionOptions region(String region) {
        this.region = region;
        return this;
    }

    public ConnectionOptions hostOverride(String hostOverride) {
        this.hostOverride = Optional.of(hostOverride);
        return this;
    }

    public ConnectionOptions readConsistencyInterval(double interval) {
        this.readConsistencyInterval = Optional.of(interval);
        return this;
    }

    public ConnectionOptions storageOptions(Map<String, String> options) {
        this.storageOptions.putAll(options);
        return this;
    }

    // Getters
    public Optional<String> getApiKey() { return apiKey; }
    public String getRegion() { return region; }
    public Optional<String> getHostOverride() { return hostOverride; }
    public Optional<Double> getReadConsistencyInterval() { return readConsistencyInterval; }
    public Map<String, String> getStorageOptions() { return storageOptions; }
}