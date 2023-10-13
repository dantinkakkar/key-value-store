package com.sahaj;

import javafx.util.Pair;

import java.io.*;
import java.util.Map;

public class KeyValueStore {

    private final Map<String, Pair<Long, String>> underlyingStore;
    private final BufferedWriter writer;
    public KeyValueStore(
            final Map<String, Pair<Long, String>> underlyingStoreImpl,
            final BufferedWriter writerImpl
    ) {
        this.underlyingStore = underlyingStoreImpl;
        this.writer = writerImpl;
    }

    public void writeValue(String key, String value) {
        underlyingStore.compute(key, (__, oldValue) -> {
            long time = System.nanoTime();
            final long oldTime = oldValue != null ? oldValue.getKey() : 0;
            try {
                writer.write(key+":"+value+":"+time);
                writer.newLine();
                return time > oldTime ? new Pair<>(time, value) : oldValue;
            } catch (IOException e) {
                e.printStackTrace();
                return oldValue;
            }
        });
    }

    public String getValue(String key) {
        return underlyingStore.get(key).getValue();
    }
}
