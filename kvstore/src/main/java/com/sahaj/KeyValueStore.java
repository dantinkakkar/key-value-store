package com.sahaj;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueStore {

    private final ConcurrentHashMap<String, String> underlyingStore;
    private final BufferedWriter writer;

    public KeyValueStore(final ConcurrentHashMap<String, String> underlyingStoreImpl, final BufferedWriter writerImpl) {
        this.underlyingStore = underlyingStoreImpl;
        this.writer = writerImpl;
    }

    synchronized public void writeValue(String key, String value) throws IOException {
        underlyingStore.put(key, value);
        writer.write(key+":"+value);
        writer.newLine();
    }

    public String getValue(String key) {
        return underlyingStore.get(key);
    }
}
