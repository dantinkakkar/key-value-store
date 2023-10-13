package com.sahaj;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import javafx.util.Pair;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class HTTPInterface {

    public static void main(String[] args) throws Exception {
        final long durabilityInMs = Long.parseLong(args[0]);
        final short storesToUse = Short.parseShort(args[1]);
        final long bufferSize = (durabilityInMs/2)*3140000 < 0 ? Integer.MAX_VALUE - 8 : (durabilityInMs/2)*3140000;
        final Map<String, Pair<Long, String>> underlyingMap = new ConcurrentHashMap<>();
        final List<File> stores = new ArrayList<>();
        final List<BufferedWriter> writers = new ArrayList<>();
        for (short i=0;i<storesToUse;i++) {
            final File file = new File("store" + i);
            if (!file.exists()) file.createNewFile();
            stores.add(file);
            writers.add(new BufferedWriter(new FileWriter(file, true),((Long) bufferSize).intValue()/storesToUse));
        }
        final ScheduledExecutorService flushService = Executors.newSingleThreadScheduledExecutor();
        flushService.scheduleAtFixedRate(new Thread(() -> {
            try {
                for(BufferedWriter w : writers) w.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }), 0, durabilityInMs/2, TimeUnit.MILLISECONDS);
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

    static class StoreHandler implements HttpHandler {
        private final KeyValueStore store;

        StoreHandler(final KeyValueStore storeImpl) {
            this.store = storeImpl;
        }

        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            final String contextPath = exchange.getRequestURI();
            final String[] parts = contextPath.split("/");
            switch (exchange.getRequestMethod().toString()) {
                case "GET" -> handleGet(exchange, parts[2]);
                case "PUT" -> handlePut(exchange, parts[2], parts[4]);
                default -> handleUnsupported(exchange);
            }
        }

        private void handlePut(final HttpServerExchange t, final String key, final String value) {
            final long currentTime = System.nanoTime();
            store.writeValue(key, value, currentTime);
            t.getResponseSender().close();
        }

        private void handleGet(final HttpServerExchange t, final String key) {
            final String value = store.getValue(key);
            t.getResponseSender().send(ByteBuffer.wrap(value.getBytes()));
        }

        private void handleUnsupported(final HttpServerExchange t) {
            t.setStatusCode(405);
            t.getResponseSender().close();
        }
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
