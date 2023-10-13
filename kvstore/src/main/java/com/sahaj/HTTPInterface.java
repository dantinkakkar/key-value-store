package com.sahaj;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.HashMap;
import java.util.concurrent.*;

public class HTTPInterface {

    public static void main(String[] args) throws Exception {
        final long durabilityInMs = Long.parseLong(args[0]);
        final long bufferSize = (durabilityInMs/2)*3140000 < 0 ? Integer.MAX_VALUE - 8 : (durabilityInMs/2)*3140000;
        final File storeFile = new File("store");
        if (!storeFile.exists()) {
            storeFile.createNewFile();
        }
        final BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(storeFile, true),
                ((Long) bufferSize).intValue());
        final ScheduledExecutorService flushService = Executors.newSingleThreadScheduledExecutor();
        flushService.scheduleAtFixedRate(new Thread(() -> {
            try {
                bufferedWriter.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }), 0, durabilityInMs/2, TimeUnit.MILLISECONDS);
        final KeyValueStore keyValueStore = new KeyValueStore(new HashMap<>(), bufferedWriter);
        Undertow server = Undertow
                .builder()
                .addHttpListener(8000, "localhost")
                .setHandler(new StoreHandler(keyValueStore))
                .build();
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                bufferedWriter.close();
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

        private void handlePut(final HttpServerExchange t, final String key, final String value) throws IOException {
            store.writeValue(key, value);
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
}
