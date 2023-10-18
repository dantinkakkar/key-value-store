package dev.dantin;

import io.undertow.Undertow;
import javafx.util.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class HTTPInterface {

    public static void main(String[] args) throws Exception {
        final long durabilityInMs = Long.parseLong(args[0]);
        final Map<String, Pair<Long, String>> underlyingMap = new TreeMap<>();
        final File wal = new File("store");
        if (!wal.exists()) wal.createNewFile();
        initializeStores(wal, underlyingMap);
        final KeyValueStore keyValueStore = durabilityInMs > 0 ?
                new KeyValueStore(underlyingMap, durabilityInMs / 2) :
                new InstantlyDurableKeyValueStore(underlyingMap, durabilityInMs / 2);
        final Undertow server = Undertow
                .builder()
                .addHttpListener(8000, "localhost")
                .setHandler(new StoreHandler(keyValueStore))
                .build();
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                keyValueStore.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }

    private static void initializeStores(final File storeFile, final Map<String, Pair<Long, String>> initStore) throws IOException {
        Files.readAllLines(Path.of(storeFile.toURI())).forEach(line -> {
            final String[] parts = line.split(":");
            final String key = parts[0];
            final String newValue = parts[1];
            final long newTime = Long.parseLong(parts[2]);
            initStore.compute(key, (k, oldPair) -> {
                final long oldTime = oldPair != null ? oldPair.getKey() : 0;
                return newTime > oldTime ? new Pair<>(newTime, newValue) : oldPair;
            });
        });
    }
}
