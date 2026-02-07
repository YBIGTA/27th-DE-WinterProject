package com.ingestion.controller;

import com.ingestion.dto.BatchIngestResponse;
import com.ingestion.dto.TaxiEvent;
import com.ingestion.service.IngestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RestController
@RequiredArgsConstructor
public class IngestionController {

    private final IngestionService ingestionService;

    /**
     * Health check endpoint for container orchestration.
     */
    @GetMapping("/health")
    public Mono<ResponseEntity<String>> health() {
        return Mono.just(ResponseEntity.ok("OK"));
    }

    /**
     * Ingest endpoint for taxi events.
     * Returns:
     * - 202 Accepted: Event queued successfully
     * - 429 Too Many Requests: Buffer full, client should retry
     * - 500 Internal Server Error: Other emit failures
     */
    @PostMapping("/ingest")
    public Mono<ResponseEntity<Void>> ingest(@RequestBody TaxiEvent event) {
        Sinks.EmitResult result = ingestionService.ingest(event);

        switch (result) {
            case OK:
                // Successfully queued
                return Mono.just(ResponseEntity.status(HttpStatus.ACCEPTED).build());

            case FAIL_OVERFLOW:
                // Buffer full - return 429 so generator can handle backpressure
                log.warn("[BACKPRESSURE] Rejected event due to buffer overflow: trip_id={}",
                         event.getTripId());
                return Mono.just(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build());

            case FAIL_NON_SERIALIZED:
                // Should not happen after service-level retry, but handle anyway
                log.warn("[CONCURRENCY] Event emission failed due to concurrent access: trip_id={}",
                         event.getTripId());
                return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build());

            default:
                // Other failures (FAIL_CANCELLED, FAIL_TERMINATED, FAIL_ZERO_SUBSCRIBER)
                log.error("[ERROR] Failed to emit event: result={}, trip_id={}",
                          result, event.getTripId());
                return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
        }
    }

    /**
     * Batch ingest endpoint for taxi events.
     * Accepts a JSON array of events.
     * Returns:
     * - 202 Accepted: All events queued successfully
     * - 207 Multi-Status: Partial success (some events dropped)
     * - 429 Too Many Requests: Buffer full, client should retry entire batch
     * - 500 Internal Server Error: Other emit failures
     */
    @PostMapping("/ingest/batch")
    public Mono<ResponseEntity<BatchIngestResponse>> ingestBatch(@RequestBody List<TaxiEvent> events) {
        if (events == null || events.isEmpty()) {
            return Mono.just(ResponseEntity.badRequest().build());
        }

        int totalEvents = events.size();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);
        List<Integer> failedIndices = new ArrayList<>();

        // Try to emit all events
        for (int i = 0; i < events.size(); i++) {
            TaxiEvent event = events.get(i);
            Sinks.EmitResult result = ingestionService.ingest(event);

            if (result == Sinks.EmitResult.OK) {
                successCount.incrementAndGet();
            } else {
                failedCount.incrementAndGet();
                failedIndices.add(i);

                if (result == Sinks.EmitResult.FAIL_OVERFLOW) {
                    // Buffer full - stop processing and return 429
                    log.warn("[BATCH_BACKPRESSURE] Buffer overflow at event {}/{}, trip_id={}",
                             i + 1, totalEvents, event.getTripId());

                    BatchIngestResponse response = new BatchIngestResponse(
                        successCount.get(),
                        failedCount.get() + (totalEvents - i - 1),  // Include remaining unprocessed
                        failedIndices
                    );
                    return Mono.just(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(response));
                }
            }
        }

        // All events processed
        if (failedCount.get() == 0) {
            // All succeeded
            BatchIngestResponse response = new BatchIngestResponse(totalEvents, 0, Collections.emptyList());
            return Mono.just(ResponseEntity.status(HttpStatus.ACCEPTED).body(response));
        } else if (successCount.get() > 0) {
            // Partial success
            log.warn("[BATCH_PARTIAL] Batch partially succeeded: {}/{} events accepted",
                     successCount.get(), totalEvents);
            BatchIngestResponse response = new BatchIngestResponse(
                successCount.get(),
                failedCount.get(),
                failedIndices
            );
            return Mono.just(ResponseEntity.status(HttpStatus.MULTI_STATUS).body(response));
        } else {
            // All failed (unlikely unless service is down)
            log.error("[BATCH_FAILED] Entire batch failed: {} events", totalEvents);
            BatchIngestResponse response = new BatchIngestResponse(0, totalEvents, failedIndices);
            return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response));
        }
    }
}
