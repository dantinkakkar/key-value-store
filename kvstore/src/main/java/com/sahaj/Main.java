package com.sahaj;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class Main {
    public static void main(String[] args) throws Exception {
        final HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/key", new TestHandler());
        server.setExecutor(null);
        server.start();
    }

    static class TestHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            if ("PUT".equals(t.getRequestMethod())) {
                final String contextPath = t.getRequestURI().toString();
                final String[] parts = contextPath.split("/");
                assert parts.length == 4; // validation, unneeded
                final String response = "key: " + parts[2] + "; value: " + parts[4];
                t.sendResponseHeaders(200, response.length());
                final OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } else {
                t.sendResponseHeaders(405, 0);
                t.close();
            }
        }
    }
}