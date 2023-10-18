package dev.dantin;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.nio.ByteBuffer;

class StoreHandler implements HttpHandler {
    private final KeyValueStore store;

    StoreHandler(final KeyValueStore storeImpl) {
        this.store = storeImpl;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws IOException {
        final String contextPath = exchange.getRequestURI();
        final String[] parts = contextPath.split("/");
        switch (exchange.getRequestMethod().toString()) {
            case "GET" -> handleGet(exchange, parts[2]);
            case "PUT" -> handlePut(exchange, parts[2], parts[4]);
            default -> handleUnsupported(exchange);
        }
    }

    private void handlePut(final HttpServerExchange t, final String key, final String value) throws IOException {
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

