package dev.dantin;

import javafx.util.Pair;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public class InstantlyDurableKeyValueStore extends KeyValueStore {
    public InstantlyDurableKeyValueStore(Map<String, Pair<Long, String>> underlyingStoreImpl, BufferedWriter writer, ScheduledExecutorService flushService, long flushDuration) {
        super(underlyingStoreImpl, writer, flushService, flushDuration);
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
}
