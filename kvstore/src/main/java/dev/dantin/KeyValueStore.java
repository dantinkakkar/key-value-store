package dev.dantin;

import javafx.util.Pair;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class KeyValueStore {

    private final Map<String, Pair<Long, String>> underlyingStore;
    private final BufferedWriter writer;
    public KeyValueStore(
            final Map<String, Pair<Long, String>> underlyingStoreImpl,
            final BufferedWriter writer
            ) {
        this.underlyingStore = underlyingStoreImpl;
        this.writer = writer;
    }

    synchronized public void writeValue(String key, String value, long time) {
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

    public String getValue(String key) {
        return underlyingStore.get(key).getValue();
    }
}
