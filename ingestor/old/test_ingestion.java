import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class test_ingestion {
    private static final class Stats {
        private final AtomicLong total = new AtomicLong();
        private final AtomicBoolean printed = new AtomicBoolean(false);
        private final java.util.Set<String> tripIds = ConcurrentHashMap.newKeySet();
        private final java.util.Set<String> pickupTripIds = ConcurrentHashMap.newKeySet();
        private final java.util.Set<String> transitTripIds = ConcurrentHashMap.newKeySet();
        private final java.util.Set<String> dropoffTripIds = ConcurrentHashMap.newKeySet();

        void record(String payload) {
            total.incrementAndGet();
            String tripId = extractJsonString(payload, "trip_id");
            if (tripId == null || tripId.isEmpty()) return;
            tripIds.add(tripId);

            String event = extractJsonString(payload, "event");
            if (event == null) return;
            switch (event) {
                case "PICKUP":
                    pickupTripIds.add(tripId);
                    break;
                case "IN_TRANSIT":
                    transitTripIds.add(tripId);
                    break;
                case "DROPOFF":
                    dropoffTripIds.add(tripId);
                    break;
                default:
                    break;
            }
        }

        void printSummary() {
            if (!printed.compareAndSet(false, true)) return;

            long totalEvents = total.get();

            System.out.println("=== Ingestion Summary ===");
            System.out.println("Total events processed: " + totalEvents);
            System.out.println("Unique trip IDs: " + tripIds.size());
            System.out.println("Trip IDs with PICKUP: " + pickupTripIds.size());
            System.out.println("Trip IDs with IN_TRANSIT: " + transitTripIds.size());
            System.out.println("Trip IDs with DROPOFF: " + dropoffTripIds.size());
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

    private static String extractJsonString(String payload, String key) {
        if (payload == null || key == null) return null;
        String needle = "\"" + key + "\"";
        int keyIndex = payload.indexOf(needle);
        if (keyIndex < 0) return null;
        int colon = payload.indexOf(':', keyIndex + needle.length());
        if (colon < 0) return null;
        int quoteStart = payload.indexOf('"', colon + 1);
        if (quoteStart < 0) return null;
        int quoteEnd = payload.indexOf('"', quoteStart + 1);
        if (quoteEnd < 0) return null;
        return payload.substring(quoteStart + 1, quoteEnd);
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
