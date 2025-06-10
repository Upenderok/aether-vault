package com.aethervault.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A durable, persistent implementation of the StorageEngine.
 * <p>
 * It uses a Write-Ahead Log (WAL) for durability. All write operations (put, delete)
 * are first appended to a log file on disk before being applied to the in-memory map.
 * On startup, the log is replayed to restore the state.
 */
public class PersistentStorageEngine implements StorageEngine {

    private final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
    private final PrintWriter walWriter; // The writer for our Write-Ahead Log file.

    // The name of our log file.
    private static final String WAL_FILE_NAME = "aether.wal";
    private static final String DELIMITER = ","; // Delimiter for our simple CSV-like log format.

    /**
     * Constructor that initializes the engine.
     * It replays the Write-Ahead Log to restore the state from disk.
     *
     * @param storageDir The directory where the WAL file is stored.
     */
    public PersistentStorageEngine(Path storageDir) {
        File walFile = storageDir.resolve(WAL_FILE_NAME).toFile();
        replayLog(walFile); // Rebuild state from the log first.

        try {
            // Open the WAL file in append mode. The 'true' flag is crucial.
            // A PrintWriter wrapped around a FileWriter gives us convenient 'println' method.
            this.walWriter = new PrintWriter(new BufferedWriter(new FileWriter(walFile, true)));
        } catch (IOException e) {
            // If we can't open the log file for writing, it's a fatal error.
            throw new RuntimeException("Failed to open Write-Ahead Log", e);
        }
    }

    /**
     * Reads the WAL file line by line and applies the operations to the in-memory map.
     */
    private void replayLog(File walFile) {
        if (!walFile.exists()) {
            System.out.println("No WAL file found. Starting with a clean state.");
            return;
        }

        System.out.println("Replaying WAL file to restore state...");
        try (BufferedReader reader = new BufferedReader(new FileReader(walFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Our log format is: COMMAND,key,value (or COMMAND,key for delete)
                String[] parts = line.split(DELIMITER, 3);
                if (parts.length < 2) continue; // Skip malformed lines.

                String command = parts[0];
                String key = parts[1];

                if ("PUT".equals(command) && parts.length == 3) {
                    String value = parts[2];
                    map.put(key, value);
                } else if ("DELETE".equals(command)) {
                    map.remove(key);
                }
            }
        } catch (IOException e) {
            // If we can't read the log, we can't guarantee state. This is a fatal error.
            throw new RuntimeException("Failed to replay Write-Ahead Log", e);
        }
        System.out.println("State restored. " + map.size() + " keys loaded.");
    }


    @Override
    public void put(String key, String value) {
        synchronized (this) { // Synchronize to ensure write-to-log and write-to-map are atomic.
            // 1. Write to log FIRST
            walWriter.println("PUT" + DELIMITER + key + DELIMITER + value);
            walWriter.flush(); // Ensure data is physically written to disk.

            // 2. Then update memory
            map.put(key, value);
        }
    }

    @Override
    public Optional<String> get(String key) {
        // Reads are fast! They come directly from memory.
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public void delete(String key) {
        synchronized (this) {
            // 1. Write to log FIRST
            walWriter.println("DELETE" + DELIMITER + key);
            walWriter.flush();

            // 2. Then update memory
            map.remove(key);
        }
    }
}