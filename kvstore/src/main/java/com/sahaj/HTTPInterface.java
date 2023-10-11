package com.sahaj;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HTTPInterface {

    public static void main(String[] args) throws Exception {
        final int DURABILITY_GUARANTEE_IN_MS = 50;
        final File storeFile = new File("store");
        if (!storeFile.exists()) {
            storeFile.createNewFile();
        }
        final BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(storeFile, true), DURABILITY_GUARANTEE_IN_MS*15728634);
        final ScheduledExecutorService flushService = Executors.newSingleThreadScheduledExecutor();
        flushService.scheduleAtFixedRate(new Thread(() -> {
            try {
                bufferedWriter.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }), 0, DURABILITY_GUARANTEE_IN_MS/2, TimeUnit.MILLISECONDS);
        final KeyValueStore keyValueStore = new KeyValueStore(new ConcurrentHashMap<>(), bufferedWriter);
        final HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/key", new StoreHandler(keyValueStore));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }

    private

    static class StoreHandler implements HttpHandler {
        private final KeyValueStore store;

        StoreHandler(final KeyValueStore storeImpl) {
            this.store = storeImpl;
        }

        @Override
        public void handle(HttpExchange t) throws IOException {
            final String contextPath = t.getRequestURI().toString();
            final String[] parts = contextPath.split("/");
            t.getResponseHeaders().add("Connection", "Keep-Alive");
            switch (t.getRequestMethod()) {
                case "GET" -> handleGet(t, parts[2]);
                case "PUT" -> handlePut(t, parts[2], parts[4]);
                default -> handleUnsupported(t);
            }
        }

        private void handlePut(final HttpExchange t, final String key, final String value) throws IOException {
            store.writeValue(key, value);
            t.sendResponseHeaders(200, 0);
            t.close();
        }

        private void handleGet(final HttpExchange t, final String key) throws IOException {
            final String value = store.getValue(key);
            t.sendResponseHeaders(200, value.getBytes().length);
            final OutputStream os = t.getResponseBody();
            os.write(value.getBytes());
            os.close();
        }

        private void handleUnsupported(final HttpExchange t) throws IOException {
            t.sendResponseHeaders(405, 0);
            t.close();
        }
    }
}
