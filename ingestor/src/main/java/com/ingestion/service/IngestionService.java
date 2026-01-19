package com.ingestion.service;

import com.ingestion.dto.TaxiEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class IngestionService {

    private final KafkaSender<String, String> kafkaSender;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic}")
    private String topicName;

    // Buffer: 10,000 events with FAIL_FAST backpressure
    private final Sinks.Many<TaxiEvent> sink = Sinks.many()
            .multicast()
            .onBackpressureBuffer(10000, false);

    // Metrics for structured logging
    private final AtomicLong eventsReceived = new AtomicLong(0);
    private final AtomicLong eventsProcessed = new AtomicLong(0);
    private final AtomicLong eventsFailed = new AtomicLong(0);
    private final AtomicLong eventsDropped = new AtomicLong(0);
    private final AtomicLong batchesSent = new AtomicLong(0);

    @PostConstruct
    public void init() {
        log.info("[STARTUP] Initializing ingestion pipeline: buffer=10000, batch=500, timeout=50ms, topic={}", topicName);

        // Pipeline: Buffer → Batch → Parallel Kafka Send with Retry
        sink.asFlux()
            .bufferTimeout(500, Duration.ofMillis(50))
            .flatMap(this::sendBatchToKafkaReactive, 4)  // 4 concurrent Kafka sends
            .doOnError(e -> log.error("[PIPELINE] Critical error in pipeline", e))
            .retry()  // Retry the entire subscription if it fails
            .subscribe(
                result -> {
                    // Success metrics logged in sendBatchToKafkaReactive
                },
                error -> log.error("[PIPELINE] Pipeline terminated with error", error),
                () -> log.info("[SHUTDOWN] Pipeline completed")
            );

        // Log metrics every 10 seconds
        Flux.interval(Duration.ofSeconds(10))
            .doOnNext(tick -> logMetrics())
            .subscribe();
    }

    /**
     * Send a batch to Kafka using Reactor Kafka with retry logic.
     * Uses parallel sends for high throughput.
     */
    private Mono<Void> sendBatchToKafkaReactive(List<TaxiEvent> batch) {
        if (batch.isEmpty()) {
            return Mono.empty();
        }

        long batchStartTime = System.nanoTime();
        int batchSize = batch.size();

        log.debug("[BATCH] Processing batch: size={}, buffer_usage={}%",
                  batchSize, getBufferUsagePercent());

        // Convert batch to SenderRecord stream
        Flux<SenderRecord<String, String, Integer>> records = Flux.fromIterable(batch)
            .index()
            .flatMap(tuple -> {
                int index = tuple.getT1().intValue();
                TaxiEvent event = tuple.getT2();

                try {
                    String key = String.valueOf(event.getTripId());
                    String value = objectMapper.writeValueAsString(event);
                    ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topicName, key, value);

                    return Mono.just(SenderRecord.create(producerRecord, index));
                } catch (Exception e) {
                    log.error("[SERIALIZATION] Failed to serialize event: trip_id={}",
                              event.getTripId(), e);
                    eventsFailed.incrementAndGet();
                    return Mono.empty();  // Skip this event
                }
            });

        // Send to Kafka with retry logic
        return kafkaSender.send(records)
            .doOnNext(result -> {
                if (result.exception() != null) {
                    log.error("[KAFKA] Send failed for record {}: {}",
                              result.correlationMetadata(),
                              result.exception().getMessage());
                    eventsFailed.incrementAndGet();
                } else {
                    eventsProcessed.incrementAndGet();
                }
            })
            .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .doBeforeRetry(signal ->
                    log.warn("[RETRY] Retrying batch send: attempt={}, error={}",
                             signal.totalRetries() + 1,
                             signal.failure().getMessage())
                )
            )
            .doOnError(e -> {
                log.error("[KAFKA] Batch send failed after retries: size={}", batchSize, e);
                eventsFailed.addAndGet(batchSize);
            })
            .then()
            .doFinally(signalType -> {
                long durationMs = (System.nanoTime() - batchStartTime) / 1_000_000;
                batchesSent.incrementAndGet();
                log.debug("[BATCH] Completed: size={}, duration_ms={}, signal={}",
                          batchSize, durationMs, signalType);
            });
    }

    /**
     * Ingest a single event. Returns emission result for controller to check.
     */
    public Sinks.EmitResult ingest(TaxiEvent event) {
        eventsReceived.incrementAndGet();
        Sinks.EmitResult result = sink.tryEmitNext(event);

        if (result == Sinks.EmitResult.FAIL_OVERFLOW) {
            eventsDropped.incrementAndGet();
            log.warn("[BACKPRESSURE] Buffer full, event dropped: trip_id={}, buffer_usage=100%",
                     event.getTripId());
        }

        return result;
    }

    /**
     * Get buffer usage percentage for monitoring.
     */
    public int getBufferUsagePercent() {
        // Estimate based on dropped events (Sinks API doesn't expose size)
        // This is a rough approximation
        long dropped = eventsDropped.get();
        if (dropped > 0) {
            return 100;  // If we've dropped any, buffer was full
        }
        // Can't accurately measure with Sinks API, return conservative estimate
        long received = eventsReceived.get();
        long processed = eventsProcessed.get();
        long pending = received - processed;

        if (pending >= 10000) return 100;
        if (pending >= 8000) return 80;
        if (pending >= 5000) return 50;
        return (int) ((pending * 100) / 10000);
    }

    /**
     * Log structured metrics for monitoring.
     */
    private void logMetrics() {
        long received = eventsReceived.get();
        long processed = eventsProcessed.get();
        long failed = eventsFailed.get();
        long dropped = eventsDropped.get();
        long batches = batchesSent.get();

        log.info("[METRICS] events_received={}, events_processed={}, events_failed={}, " +
                 "events_dropped={}, batches_sent={}, buffer_usage={}%, " +
                 "success_rate={}",
                 received, processed, failed, dropped, batches,
                 getBufferUsagePercent(),
                 received > 0 ? String.format("%.2f%%", (processed * 100.0 / received)) : "N/A");
    }

    @PreDestroy
    public void shutdown() {
        log.info("[SHUTDOWN] Flushing remaining events...");

        // Complete the sink to signal no more events
        sink.tryEmitComplete();

        // Give pipeline time to flush (max 5 seconds)
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Close Kafka sender
        kafkaSender.close();

        logMetrics();
        log.info("[SHUTDOWN] Ingestion service shut down gracefully");
    }
}
