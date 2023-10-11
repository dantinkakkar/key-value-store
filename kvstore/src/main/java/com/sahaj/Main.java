package com.sahaj;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws Exception {
        final HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/key", new TestHandler(new HashMap<>()));
        server.setExecutor(null);
        server.start();
    }

    static class TestHandler implements HttpHandler {
        private final Map<String, String> store;

        TestHandler(final Map<String, String> storeImpl) {
            this.store = storeImpl;
        }

        @Override
        public void handle(HttpExchange t) throws IOException {
            if ("PUT".equals(t.getRequestMethod())) {
                final String contextPath = t.getRequestURI().toString();
                final String[] parts = contextPath.split("/");
                store.put(parts[2], parts[4]);
                t.sendResponseHeaders(200, 0);
                t.close();
            } else {
                t.sendResponseHeaders(405, 0);
                t.close();
            }
        }
    }
}