package com.aethervault.storage;

import java.util.Optional;

/**
 * Defines the contract for the core storage component of an AetherVault node.
 * This is the lowest level of data management.
 */
public interface StorageEngine {

    /**
     * Stores a key-value pair.
     *
     * @param key   The key for the data.
     * @param value The value to be stored.
     */
    void put(String key, String value);

    /**
     * Retrieves the value associated with a key.
     *
     * @param key The key to look up.
     * @return An Optional containing the value if the key exists, otherwise an empty Optional.
     */
    Optional<String> get(String key);

    /**
     * Deletes a key-value pair.
     *
     * @param key The key to be deleted.
     */
    void delete(String key);
}