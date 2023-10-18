package dev.dantin;

import javafx.util.Pair;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;

public class InstantlyDurableKVStore extends KeyValueStore {
    private final BufferedWriter writer;
    public InstantlyDurableKVStore(Map<String, Pair<Long, String>> underlyingStoreImpl, BufferedWriter writer) {
        super(underlyingStoreImpl, writer);
        this.writer = writer;
    }

    @Override
    synchronized public void writeValue(String key, String value, long time) {
        super.writeValue(key, value, time);
        try {
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
