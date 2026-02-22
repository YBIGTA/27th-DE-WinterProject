package com.ingestion.service;

import com.ingestion.config.IngestorTuningProperties;
import com.ingestion.dto.TaxiEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

@Slf4j
@Service
@RequiredArgsConstructor
public class IngestionService {

    private final KafkaSender<String, String> kafkaSender;
    private final ObjectMapper objectMapper;
    private final IngestorTuningProperties tuning;
    private final MeterRegistry meterRegistry;

    @Value("${app.kafka.topic:taxi-event-data}")
    private String topicName;

    @Value("${app.dlq.filepath:dead_letter_queue.jsonl}")
    private String dlqFilePath;

    private Sinks.Many<TaxiEvent> sink;
    private int bufferSize;
    private DeadLetterQueue dlq;

    // Metrics for structured logging
    private final AtomicLong eventsReceived = new AtomicLong(0);
    private final AtomicLong eventsProcessed = new AtomicLong(0);
    private final AtomicLong eventsFailed = new AtomicLong(0);
    private final AtomicLong eventsDropped = new AtomicLong(0);
    private final AtomicLong batchesSent = new AtomicLong(0);
    private final AtomicLong emitTotal = new AtomicLong(0);
    private final AtomicLong emitNonSerialized = new AtomicLong(0);
    private final AtomicLong emitOverflow = new AtomicLong(0);
    private final AtomicLong emitOtherFail = new AtomicLong(0);
    private final AtomicLong recordsEnqueued = new AtomicLong(0);
    private final AtomicLong recordsCompleted = new AtomicLong(0);
    private final AtomicLong retryAttempts = new AtomicLong(0);
    private final AtomicLong retryExhausted = new AtomicLong(0);
    private final AtomicLong dlqWriteErrors = new AtomicLong(0);
    private final AtomicLong pipelineResubscribe = new AtomicLong(0);
    private final AtomicLong pipelineErrors = new AtomicLong(0);

    // Bounded retry policy for FAIL_NON_SERIALIZED to avoid CPU spin under contention.
    private static final int EMIT_RETRY_LIMIT = 10;
    private static final long EMIT_RETRY_INITIAL_BACKOFF_NS = 50_000L; // 50us
    private static final long EMIT_RETRY_MAX_BACKOFF_NS = 2_000_000L;  // 2ms
    private static final int SENDER_MAX_INFLIGHT = 1024;

    private Counter eventsReceivedCounter;
    private Counter eventsProcessedCounter;
    private Counter eventsFailedCounter;
    private Counter eventsDroppedCounter;
    private Counter batchesSentCounter;
    private Counter emitTotalCounter;
    private Counter emitNonSerializedCounter;
    private Counter emitOverflowCounter;
    private Counter emitOtherFailCounter;
    private Counter recordsEnqueuedCounter;
    private Counter recordsCompletedCounter;
    private Counter retryAttemptsCounter;
    private Counter retryExhaustedCounter;
    private Counter dlqWriteErrorsCounter;
    private Counter pipelineResubscribeCounter;
    private Counter pipelineErrorsCounter;
    private Timer batchSendTimer;
    private DistributionSummary batchSizeSummary;

    @PostConstruct
    public void init() {
        dlq = initializeDlqWithFallback(dlqFilePath);

        bufferSize = tuning.getBuffer().getSize();
        int batchSize = tuning.getBatch().getSize();
        long batchTimeoutMs = tuning.getBatch().getTimeoutMs();
        int sendConcurrency = tuning.getKafka().getSendConcurrency();
        long metricsIntervalSec = tuning.getMetrics().getIntervalSec();

        // Buffer: configured size with FAIL_FAST backpressure
        sink = Sinks.many()
            .multicast()
            .onBackpressureBuffer(bufferSize, false);

        initMeters();

        log.info("[STARTUP] Initializing ingestion pipeline: buffer={}, batch={}, timeout={}ms, topic={}",
                 bufferSize, batchSize, batchTimeoutMs, topicName);

        // Pipeline: Buffer → Batch → Parallel Kafka Send with Retry
        sink.asFlux()
            .bufferTimeout(batchSize, Duration.ofMillis(batchTimeoutMs))
            .flatMap(this::sendBatchToKafkaReactive, sendConcurrency)
            .doOnError(e -> {
                pipelineErrors.incrementAndGet();
                pipelineErrorsCounter.increment();
                log.error("[PIPELINE] Critical error in pipeline", e);
            })
            .retryWhen(Retry.indefinitely().doBeforeRetry(signal -> {
                pipelineResubscribe.incrementAndGet();
                pipelineResubscribeCounter.increment();
                log.warn("[PIPELINE] Resubscribing pipeline after error: attempt={}, error={}",
                         signal.totalRetries() + 1,
                         signal.failure().getMessage());
            }))
            .subscribe(
                result -> {
                    // Success metrics logged in sendBatchToKafkaReactive
                },
                error -> log.error("[PIPELINE] Pipeline terminated with error", error),
                () -> log.info("[SHUTDOWN] Pipeline completed")
            );

        // Log metrics every 10 seconds
        Flux.interval(Duration.ofSeconds(metricsIntervalSec))
            .doOnNext(tick -> logMetrics())
            .subscribe();
    }

    private DeadLetterQueue initializeDlqWithFallback(String configuredPath) {
        try {
            return new DeadLetterQueue(configuredPath);
        } catch (IOException primaryError) {
            String fileName = Paths.get(configuredPath).getFileName().toString();
            String fallbackPath = System.getProperty("java.io.tmpdir") + "/" + fileName;
            log.warn("[DLQ] Failed to open configured path. Falling back to tmp path. configured={}, fallback={}, reason={}",
                     configuredPath, fallbackPath, primaryError.getMessage());
            try {
                return new DeadLetterQueue(fallbackPath);
            } catch (IOException fallbackError) {
                throw new RuntimeException(
                    "Failed to initialize dead letter queue. configured=" + configuredPath + ", fallback=" + fallbackPath,
                    fallbackError
                );
            }
        }
    }

    private void initMeters() {
        eventsReceivedCounter = meterRegistry.counter("ingestor.events.received.total");
        eventsProcessedCounter = meterRegistry.counter("ingestor.events.processed.total");
        eventsFailedCounter = meterRegistry.counter("ingestor.events.failed.total");
        eventsDroppedCounter = meterRegistry.counter("ingestor.events.dropped.total");
        batchesSentCounter = meterRegistry.counter("ingestor.batches.sent.total");
        emitTotalCounter = meterRegistry.counter("ingestor.emit.total");
        emitNonSerializedCounter = meterRegistry.counter("ingestor.emit.non_serialized.total");
        emitOverflowCounter = meterRegistry.counter("ingestor.emit.overflow.total");
        emitOtherFailCounter = meterRegistry.counter("ingestor.emit.other_fail.total");
        recordsEnqueuedCounter = meterRegistry.counter("ingestor.kafka.records.enqueued.total");
        recordsCompletedCounter = meterRegistry.counter("ingestor.kafka.records.completed.total");
        retryAttemptsCounter = meterRegistry.counter("ingestor.retry.attempts.total");
        retryExhaustedCounter = meterRegistry.counter("ingestor.retry.exhausted.total");
        dlqWriteErrorsCounter = meterRegistry.counter("ingestor.dlq.write_errors.total");
        pipelineResubscribeCounter = meterRegistry.counter("ingestor.pipeline.resubscribe.total");
        pipelineErrorsCounter = meterRegistry.counter("ingestor.pipeline.errors.total");

        batchSendTimer = Timer.builder("ingestor.kafka.batch.send.duration")
            .description("Kafka batch send duration")
            .register(meterRegistry);
        batchSizeSummary = DistributionSummary.builder("ingestor.kafka.batch.size")
            .description("Kafka batch size")
            .baseUnit("records")
            .register(meterRegistry);

        Gauge.builder("ingestor.sink.buffer.usage.percent", this, IngestionService::getBufferUsagePercent)
            .description("Estimated sink buffer usage percent")
            .register(meterRegistry);
        Gauge.builder("ingestor.sink.pending.estimate", this, IngestionService::getSinkPendingEstimate)
            .description("Estimated pending events in sink")
            .register(meterRegistry);
        Gauge.builder("ingestor.kafka.records.inflight.estimate", this, IngestionService::getRecordsInflightEstimate)
            .description("Estimated in-flight Kafka records waiting for completion")
            .register(meterRegistry);
        Gauge.builder("ingestor.kafka.sender.max_inflight", () -> SENDER_MAX_INFLIGHT)
            .description("Configured max in-flight records in Reactor Kafka sender")
            .register(meterRegistry);
        Gauge.builder("ingestor.dlq.total_written", this, IngestionService::getDlqTotalWritten)
            .description("Total records written to DLQ")
            .register(meterRegistry);
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
        AtomicLong completedInBatch = new AtomicLong(0);

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
                    recordsEnqueued.incrementAndGet();
                    recordsEnqueuedCounter.increment();

                    return Mono.just(SenderRecord.create(producerRecord, index));
                } catch (Exception e) {
                    log.error("[SERIALIZATION] Failed to serialize event: trip_id={}",
                              event.getTripId(), e);
                    if (!dlq.write(event, e)) {
                        dlqWriteErrors.incrementAndGet();
                        dlqWriteErrorsCounter.increment();
                    }
                    eventsFailed.incrementAndGet();
                    eventsFailedCounter.increment();
                    return Mono.empty();  // Skip for Kafka, but persisted in DLQ
                }
            });

        // Send to Kafka with retry logic
        return kafkaSender.send(records)
            .doOnNext(result -> {
                completedInBatch.incrementAndGet();
                recordsCompleted.incrementAndGet();
                recordsCompletedCounter.increment();
                if (result.exception() != null) {
                    log.error("[KAFKA] Send failed for record {}: {}",
                              result.correlationMetadata(),
                              result.exception().getMessage());
                    eventsFailed.incrementAndGet();
                    eventsFailedCounter.increment();
                } else {
                    eventsProcessed.incrementAndGet();
                    eventsProcessedCounter.increment();
                }
            })
            .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                .maxBackoff(Duration.ofSeconds(2))
                .doBeforeRetry(signal -> {
                    retryAttempts.incrementAndGet();
                    retryAttemptsCounter.increment();
                    log.warn("[RETRY] Retrying batch send: attempt={}, error={}",
                             signal.totalRetries() + 1,
                             signal.failure().getMessage());
                })
            )
            .doOnError(e -> {
                retryExhausted.incrementAndGet();
                retryExhaustedCounter.increment();
                long unresolved = Math.max(0, batchSize - completedInBatch.get());
                if (unresolved > 0) {
                    eventsFailed.addAndGet(unresolved);
                    eventsFailedCounter.increment((double) unresolved);
                }
                log.error("[KAFKA] Batch send failed after retries: size={}", batchSize, e);
            })
            .then()
            .doFinally(signalType -> {
                long durationNs = System.nanoTime() - batchStartTime;
                batchesSent.incrementAndGet();
                batchesSentCounter.increment();
                batchSendTimer.record(durationNs, TimeUnit.NANOSECONDS);
                batchSizeSummary.record(batchSize);
                log.debug("[BATCH] Completed: size={}, duration_ms={}, signal={}",
                          batchSize, durationNs / 1_000_000, signalType);
            });
    }

    /**
     * Ingest a single event. Returns emission result for controller to check.
     */
    public Sinks.EmitResult ingest(TaxiEvent event) {
        eventsReceived.incrementAndGet();
        eventsReceivedCounter.increment();
        emitTotal.incrementAndGet();
        emitTotalCounter.increment();

        // Bounded retries with tiny backoff for concurrent sink emission.
        Sinks.EmitResult result = sink.tryEmitNext(event);
        int retryCount = 0;
        long backoffNs = EMIT_RETRY_INITIAL_BACKOFF_NS;

        while (result == Sinks.EmitResult.FAIL_NON_SERIALIZED && retryCount < EMIT_RETRY_LIMIT) {
            LockSupport.parkNanos(backoffNs);
            backoffNs = Math.min(backoffNs * 2, EMIT_RETRY_MAX_BACKOFF_NS);
            result = sink.tryEmitNext(event);
            retryCount++;
        }

        if (result == Sinks.EmitResult.FAIL_OVERFLOW) {
            eventsDropped.incrementAndGet();
            eventsDroppedCounter.increment();
            emitOverflow.incrementAndGet();
            emitOverflowCounter.increment();
            log.warn("[BACKPRESSURE] Buffer full, event dropped: trip_id={}, buffer_usage=100%",
                     event.getTripId());
        } else if (result == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
            emitNonSerialized.incrementAndGet();
            emitNonSerializedCounter.increment();
            log.warn("[CONCURRENCY] Event emission failed after retries: trip_id={}",
                     event.getTripId());
        } else if (result != Sinks.EmitResult.OK) {
            emitOtherFail.incrementAndGet();
            emitOtherFailCounter.increment();
            log.error("[EMIT_ERROR] Failed to emit event: result={}, trip_id={}",
                      result, event.getTripId());
        } else if (retryCount > 0) {
            log.debug("[EMIT_RETRY] Succeeded after {} retries: trip_id={}",
                      retryCount, event.getTripId());
        }

        return result;
    }

    public long getSinkPendingEstimate() {
        long pending = eventsReceived.get() - eventsProcessed.get() - eventsFailed.get() - eventsDropped.get();
        return Math.max(0, pending);
    }

    public long getRecordsInflightEstimate() {
        long inflight = recordsEnqueued.get() - recordsCompleted.get();
        return Math.max(0, inflight);
    }

    /**
     * Get buffer usage percentage for monitoring.
     */
    public int getBufferUsagePercent() {
        long pending = getSinkPendingEstimate();

        if (bufferSize <= 0) return 0;
        if (pending >= bufferSize) return 100;
        if (pending >= Math.round(bufferSize * 0.8)) return 80;
        if (pending >= Math.round(bufferSize * 0.5)) return 50;
        return (int) ((pending * 100) / bufferSize);
    }

    public long getDlqTotalWritten() {
        return dlq != null ? dlq.getTotalWritten() : 0;
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
        long emitTotalValue = emitTotal.get();
        long emitNonSerializedValue = emitNonSerialized.get();
        long emitOverflowValue = emitOverflow.get();
        long emitOtherFailValue = emitOtherFail.get();
        long enqueued = recordsEnqueued.get();
        long completed = recordsCompleted.get();
        long inflight = getRecordsInflightEstimate();
        long retryAttemptsValue = retryAttempts.get();
        long retryExhaustedValue = retryExhausted.get();
        long dlqWriteErrorsValue = dlqWriteErrors.get();
        long pending = getSinkPendingEstimate();

        log.info("[METRICS] events_received={}, events_processed={}, events_failed={}, " +
                 "events_dropped={}, emit_total={}, emit_non_serialized={}, emit_overflow={}, " +
                 "emit_other_fail={}, batches_sent={}, sink_pending={}, buffer_usage={}%, " +
                 "records_enqueued={}, records_completed={}, records_inflight={}, " +
                 "retry_attempts={}, retry_exhausted={}, dlq_written={}, dlq_write_errors={}, success_rate={}",
                 received, processed, failed, dropped,
                 emitTotalValue, emitNonSerializedValue, emitOverflowValue, emitOtherFailValue,
                 batches,
                 pending, getBufferUsagePercent(),
                 enqueued, completed, inflight,
                 retryAttemptsValue, retryExhaustedValue, getDlqTotalWritten(), dlqWriteErrorsValue,
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

        // Close DLQ file
        if (dlq != null) {
            dlq.close();
        }

        logMetrics();
        log.info("[SHUTDOWN] Ingestion service shut down gracefully");
    }
}
