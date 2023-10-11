package com.sahaj;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class KeyValueStore {

    public static void main(String[] args) throws Exception {
        final HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/key", new StoreHandler(new HashMap<>()));
        server.setExecutor(null);
        server.start();
    }

    static class StoreHandler implements HttpHandler {
        private final Map<String, String> store;

        StoreHandler(final Map<String, String> storeImpl) {
            this.store = storeImpl;
        }

        @Override
        public void handle(HttpExchange t) throws IOException {
            final String contextPath = t.getRequestURI().toString();
            final String[] parts = contextPath.split("/");
            switch (t.getRequestMethod()) {
                case "GET" -> handleGet(t, parts[2]);
                case "PUT" -> handlePut(t, parts[2], parts[4]);
                default -> handleUnsupported(t);
            }
        }

        private void handlePut(final HttpExchange t, final String key, final String value) throws IOException {
            store.put(key, value);
            t.sendResponseHeaders(200, 0);
            t.close();
        }

        private void handleGet(final HttpExchange t, final String key) throws IOException {
            final String value = store.get(key);
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