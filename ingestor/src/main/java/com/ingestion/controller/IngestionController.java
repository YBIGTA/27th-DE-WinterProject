package com.ingestion.controller;

import com.ingestion.dto.TaxiEvent;
import com.ingestion.service.IngestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Slf4j
@RestController
@RequiredArgsConstructor
public class IngestionController {

    private final IngestionService ingestionService;

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

            default:
                // Other failures (FAIL_CANCELLED, FAIL_TERMINATED, FAIL_ZERO_SUBSCRIBER)
                log.error("[ERROR] Failed to emit event: result={}, trip_id={}",
                          result, event.getTripId());
                return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
        }
    }
}
