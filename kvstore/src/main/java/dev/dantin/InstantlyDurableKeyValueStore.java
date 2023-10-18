package dev.dantin;

import javafx.util.Pair;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class InstantlyDurableKeyValueStore extends KeyValueStore {
    public InstantlyDurableKeyValueStore(Map<String, Pair<Long, String>> underlyingStoreImpl, long flushDuration) throws IOException {
        super(underlyingStoreImpl, flushDuration);
    }

    @Override
    public void doWithWriter(BufferedWriter writer) {
        try {
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void initWalWriter() throws IOException {
        writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(
                Path.of("store"), StandardOpenOption.APPEND, StandardOpenOption.DSYNC
        )), 100);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
