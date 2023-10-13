package com.sahaj;

import javafx.util.Pair;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class KeyValueStore {

    private final Map<String, Pair<Long, String>> underlyingStore;
    private final List<BufferedWriter> writers;
    public KeyValueStore(
            final Map<String, Pair<Long, String>> underlyingStoreImpl,
            final List<BufferedWriter> writers
            ) {
        this.underlyingStore = underlyingStoreImpl;
        this.writers = writers;
    }

    public void writeValue(String key, String value, long time) {
        BufferedWriter writer = writers.get(ThreadLocalRandom.current().nextInt(0, writers.size()));
        synchronized (writer) {
            underlyingStore.compute(key, (__, oldValue) -> {
                final long oldTime = oldValue != null ? oldValue.getKey() : 0;
                try {
                    writer.write(key + ":" + value + ":" + time);
                    writer.newLine();
                    return time > oldTime ? new Pair<>(time, value) : oldValue;
                } catch (IOException e) {
                    e.printStackTrace();
                    return oldValue;
                }
            });
        }
    }

    public String getValue(String key) {
        return underlyingStore.get(key).getValue();
    }
}
