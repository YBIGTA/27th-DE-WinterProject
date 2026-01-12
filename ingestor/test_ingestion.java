import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class test_ingestion {
    private static final class Stats {
        private final ConcurrentHashMap<String, AtomicLong> counts = new ConcurrentHashMap<>();
        private final AtomicLong total = new AtomicLong();
        private final AtomicBoolean printed = new AtomicBoolean(false);

        void record(String payload) {
            total.incrementAndGet();
            counts.computeIfAbsent(payload, key -> new AtomicLong()).incrementAndGet();
        }

        void printSummary() {
            if (!printed.compareAndSet(false, true)) return;

            long totalEvents = total.get();
            long uniqueEvents = 0;
            long repeatedPayloads = 0;
            long repeatedEvents = 0;

            for (Map.Entry<String, AtomicLong> entry : counts.entrySet()) {
                long count = entry.getValue().get();
                if (count == 1) {
                    uniqueEvents++;
                } else if (count > 1) {
                    repeatedPayloads++;
                    repeatedEvents += count;
                }
            }

            long uniquePayloads = counts.size();
            long duplicateOccurrences = repeatedEvents - repeatedPayloads;

            System.out.println("=== Ingestion Summary ===");
            System.out.println("Total events processed: " + totalEvents);
            System.out.println("Unique payloads: " + uniquePayloads);
            System.out.println("Unique events (count==1): " + uniqueEvents);
            System.out.println("Repeated payloads (distinct): " + repeatedPayloads);
            System.out.println("Repeated events (total): " + repeatedEvents);
            System.out.println("Duplicate occurrences (beyond first): " + duplicateOccurrences);
        }
    }

    public static void main(String[] args) throws Exception {
        int port = 8080;
        String path = "/ingest";
        if (args.length >= 1) {
            port = Integer.parseInt(args[0]);
        }
        if (args.length >= 2) {
            path = args[1];
        }

        Stats stats = new Stats();
        CountDownLatch done = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        server.setExecutor(executor);

        HttpHandler ingestHandler = exchange -> handleIngest(exchange, stats);
        server.createContext(path, ingestHandler);
        server.createContext("/shutdown", exchange -> handleShutdown(exchange, server, done));
        server.createContext("/", exchange -> handleNotFound(exchange));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stats.printSummary();
            server.stop(0);
            executor.shutdownNow();
            done.countDown();
        }));

        server.start();
        System.out.println("Ingestion server listening on http://localhost:" + port + path);
        System.out.println("Send POST payloads to " + path + ". Stop with Ctrl+C or GET /shutdown.");

        done.await();
        stats.printSummary();
        executor.shutdownNow();
    }

    private static void handleIngest(HttpExchange exchange, Stats stats) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            writeResponse(exchange, 405, "Method Not Allowed\n");
            return;
        }
        String payload = readBody(exchange.getRequestBody());
        String timestamp = Instant.now().toString();
        String remote = exchange.getRemoteAddress() != null ? exchange.getRemoteAddress().toString() : "unknown";
        System.out.println("[" + timestamp + "] " + remote + " " + payload);
        stats.record(payload);
        writeResponse(exchange, 200, "OK\n");
    }

    private static void handleShutdown(HttpExchange exchange, HttpServer server, CountDownLatch done) throws IOException {
        writeResponse(exchange, 200, "Shutting down\n");
        server.stop(0);
        done.countDown();
    }

    private static void handleNotFound(HttpExchange exchange) throws IOException {
        writeResponse(exchange, 404, "Not Found\n");
    }

    private static String readBody(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[4096];
        int read;
        while ((read = in.read(data)) != -1) {
            buffer.write(data, 0, read);
        }
        return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }

    private static void writeResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
