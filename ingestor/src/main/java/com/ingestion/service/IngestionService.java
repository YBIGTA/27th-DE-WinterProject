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

    // 배치 설정
    private static final int BATCH_SIZE = 500;
    private static final int BATCH_TIMEOUT_MS = 100;  // 50ms → 100ms로 증가
    private static final int BUFFER_SIZE = 10000;
    private static final int KAFKA_CONCURRENCY = 4;

    // Buffer: 10,000 events with FAIL_FAST backpressure
    private final Sinks.Many<TaxiEvent> sink = Sinks.many()
            .multicast()
            .onBackpressureBuffer(BUFFER_SIZE, false);

    // Metrics
    private final AtomicLong eventsReceived = new AtomicLong(0);
    private final AtomicLong eventsProcessed = new AtomicLong(0);
    private final AtomicLong eventsFailed = new AtomicLong(0);
    private final AtomicLong eventsDropped = new AtomicLong(0);
    private final AtomicLong batchesSent = new AtomicLong(0);

    @PostConstruct
    public void init() {
        log.info("[STARTUP] Initializing ingestion pipeline: buffer={}, batch={}, timeout={}ms, concurrency={}, topic={}",
                BUFFER_SIZE, BATCH_SIZE, BATCH_TIMEOUT_MS, KAFKA_CONCURRENCY, topicName);

        // Pipeline: Buffer → Batch → Parallel Kafka Send with Retry
        sink.asFlux()
            .bufferTimeout(BATCH_SIZE, Duration.ofMillis(BATCH_TIMEOUT_MS))
            .filter(batch -> !batch.isEmpty())
            .flatMap(this::sendBatchToKafka, KAFKA_CONCURRENCY)
            .onErrorContinue((error, obj) -> {
                log.error("[PIPELINE] Error processing batch, continuing: {}", error.getMessage());
            })
            .subscribe(
                result -> {},
                error -> log.error("[PIPELINE] Pipeline terminated unexpectedly", error),
                () -> log.info("[SHUTDOWN] Pipeline completed")
            );

        // Log metrics every 10 seconds
        Flux.interval(Duration.ofSeconds(10))
            .doOnNext(tick -> logMetrics())
            .subscribe();
    }

    /**
     * Send a batch to Kafka with retry logic at batch level.
     */
    private Mono<Long> sendBatchToKafka(List<TaxiEvent> batch) {
        if (batch.isEmpty()) {
            return Mono.just(0L);
        }

        long batchStartTime = System.nanoTime();
        int batchSize = batch.size();
        long batchId = batchesSent.incrementAndGet();

        log.debug("[BATCH-{}] Processing: size={}", batchId, batchSize);

        // Convert batch to SenderRecord stream
        Flux<SenderRecord<String, String, Long>> records = Flux.fromIterable(batch)
            .flatMap(event -> {
                try {
                    String key = event.getTripId() != null
                        ? String.valueOf(event.getTripId())
                        : "unknown";
                    String value = objectMapper.writeValueAsString(event);
                    ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topicName, key, value);
                    return Mono.just(SenderRecord.create(producerRecord, batchId));
                } catch (Exception e) {
                    log.error("[SERIALIZATION] Failed to serialize event: trip_id={}, error={}",
                              event.getTripId(), e.getMessage());
                    eventsFailed.incrementAndGet();
                    return Mono.empty();
                }
            });

        // Send to Kafka with batch-level retry
        return kafkaSender.send(records)
            .doOnNext(result -> {
                if (result.exception() != null) {
                    log.warn("[KAFKA] Send failed for record: {}", result.exception().getMessage());
                    eventsFailed.incrementAndGet();
                } else {
                    eventsProcessed.incrementAndGet();
                }
            })
            .count()
            .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .filter(this::isRetryableException)
                .doBeforeRetry(signal ->
                    log.warn("[RETRY] Batch-{} retry attempt {}: {}",
                             batchId, signal.totalRetries() + 1,
                             signal.failure().getMessage())
                )
            )
            .doOnSuccess(count -> {
                long durationMs = (System.nanoTime() - batchStartTime) / 1_000_000;
                log.debug("[BATCH-{}] Completed: sent={}, duration_ms={}", batchId, count, durationMs);
            })
            .doOnError(e -> {
                log.error("[KAFKA] Batch-{} failed after retries: {}", batchId, e.getMessage());
                eventsFailed.addAndGet(batchSize);
            })
            .onErrorReturn(0L);
    }

    /**
     * Determine if an exception is retryable.
     */
    private boolean isRetryableException(Throwable throwable) {
        // Retry on network/timeout errors, not on serialization errors
        String message = throwable.getMessage();
        if (message == null) return true;
        return !message.contains("Serialization") && !message.contains("serialize");
    }

    /**
     * Ingest a single event. Returns emission result for controller to check.
     */
    public Sinks.EmitResult ingest(TaxiEvent event) {
        eventsReceived.incrementAndGet();
        Sinks.EmitResult result = sink.tryEmitNext(event);

        if (result == Sinks.EmitResult.FAIL_OVERFLOW) {
            eventsDropped.incrementAndGet();
            log.warn("[BACKPRESSURE] Buffer full, event dropped: trip_id={}", event.getTripId());
        }

        return result;
    }

    /**
     * Get buffer usage percentage for monitoring.
     */
    public int getBufferUsagePercent() {
        long received = eventsReceived.get();
        long processed = eventsProcessed.get();
        long failed = eventsFailed.get();
        long dropped = eventsDropped.get();

        long pending = received - processed - failed - dropped;
        if (pending <= 0) return 0;
        if (pending >= BUFFER_SIZE) return 100;
        return (int) ((pending * 100) / BUFFER_SIZE);
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

        double successRate = received > 0 ? (processed * 100.0 / received) : 0;

        log.info("[METRICS] received={}, processed={}, failed={}, dropped={}, batches={}, buffer={}%, success_rate={:.2f}%",
                 received, processed, failed, dropped, batches,
                 getBufferUsagePercent(), successRate);
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
