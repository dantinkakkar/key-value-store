package dev.dantin;

import io.undertow.Undertow;
import javafx.util.Pair;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HTTPInterface {

    public static void main(String[] args) throws Exception {
        final long durabilityInMs = Long.parseLong(args[0]);
        final short storesToUse = Short.parseShort(args[1]);
        final long bufferSize = (durabilityInMs / 2) * 3140000 < 0 ? Integer.MAX_VALUE - 8 : (durabilityInMs / 2) * 3140000;
        final Map<String, Pair<Long, String>> underlyingMap = new ConcurrentHashMap<>();
        final List<File> stores = new ArrayList<>();
        final List<BufferedWriter> writers = new ArrayList<>();
        for (short i = 0; i < storesToUse; i++) {
            final File file = new File("store" + i);
            if (!file.exists()) file.createNewFile();
            stores.add(file);
            final OutputStreamWriter osWriter = new OutputStreamWriter(Files.newOutputStream(Path.of(file.toURI()), StandardOpenOption.APPEND, StandardOpenOption.DSYNC));
            final BufferedWriter writer = new BufferedWriter(osWriter, ((Long) bufferSize).intValue() / storesToUse);
            writers.add(writer);
            initializeStores(file, underlyingMap);
        }
        final ScheduledExecutorService flushService = Executors.newSingleThreadScheduledExecutor();
        flushService.scheduleAtFixedRate(new Thread(() -> {
            try {
                for (BufferedWriter w : writers) w.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }), 0, durabilityInMs / 2, TimeUnit.MILLISECONDS);
        final KeyValueStore keyValueStore = new KeyValueStore(underlyingMap, writers);
        final Undertow server = Undertow
                .builder()
                .addHttpListener(8000, "localhost")
                .setHandler(new StoreHandler(keyValueStore))
                .build();
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                for (BufferedWriter w : writers) w.close();
                flushService.close();
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
