package com.aethervault.storage;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A simple, non-persistent implementation of the StorageEngine that holds all data in memory.
 * This implementation is thread-safe.
 */
public class InMemoryStorageEngine implements StorageEngine {

    // We use ConcurrentHashMap because it's designed for high-concurrency access
    // from multiple network threads without issues.
    private final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    @Override
    public void put(String key, String value) {
        if (key == null || value == null) {
            // Or throw an IllegalArgumentException, a design choice.
            return;
        }
        map.put(key, value);
        System.out.println("PUT: key=" + key + ", value=" + value); // Simple logging for now
    }

    @Override
    public Optional<String> get(String key) {
        // Optional.ofNullable() elegantly handles cases where the key doesn't exist.
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public void delete(String key) {
        if (key == null) {
            return;
        }
        map.remove(key);
        System.out.println("DELETE: key=" + key); // Simple logging
    }
}