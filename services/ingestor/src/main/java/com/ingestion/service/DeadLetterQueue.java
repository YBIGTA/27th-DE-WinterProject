package com.ingestion.service;

import com.ingestion.dto.TaxiEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class DeadLetterQueue {

    private final BufferedWriter writer;
    private final AtomicLong totalWritten = new AtomicLong(0);

    public DeadLetterQueue(String filePath) throws IOException {
        this.writer = new BufferedWriter(new FileWriter(filePath, true));
        log.info("[DLQ] Dead letter queue initialized: path={}", filePath);
    }

    public synchronized boolean write(TaxiEvent event, Exception error) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            error.printStackTrace(pw);
            String stackTrace = sw.toString();

            String line = "{" +
                "\"timestamp\":\"" + Instant.now().toString() + "\"," +
                "\"tripId\":" + (event.getTripId() != null ? event.getTripId() : "null") + "," +
                "\"eventData\":\"" + escapeJson(event.toString()) + "\"," +
                "\"errorClass\":\"" + escapeJson(error.getClass().getSimpleName()) + "\"," +
                "\"errorMessage\":\"" + escapeJson(error.getMessage() != null ? error.getMessage() : "") + "\"," +
                "\"stackTrace\":\"" + escapeJson(stackTrace) + "\"" +
                "}";

            writer.write(line);
            writer.newLine();
            writer.flush();
            totalWritten.incrementAndGet();

            log.debug("[DLQ] Event written to dead letter queue: trip_id={}, error={}",
                      event.getTripId(), error.getClass().getSimpleName());
            return true;
        } catch (IOException e) {
            log.error("[DLQ] Failed to write to dead letter queue: trip_id={}",
                      event.getTripId(), e);
            return false;
        }
    }

    public long getTotalWritten() {
        return totalWritten.get();
    }

    public synchronized void close() {
        try {
            writer.close();
            log.info("[DLQ] Dead letter queue closed: total_written={}", totalWritten.get());
        } catch (IOException e) {
            log.error("[DLQ] Failed to close dead letter queue", e);
        }
    }

    private static String escapeJson(String value) {
        if (value == null) return "";
        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }
}
