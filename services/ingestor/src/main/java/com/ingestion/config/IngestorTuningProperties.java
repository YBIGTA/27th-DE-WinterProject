package com.ingestion.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "app.tuning")
public class IngestorTuningProperties {

    private Buffer buffer = new Buffer();
    private Batch batch = new Batch();
    private Kafka kafka = new Kafka();
    private Metrics metrics = new Metrics();

    @Data
    public static class Buffer {
        private int size = 30000;
    }

    @Data
    public static class Batch {
        private int size = 500;
        private long timeoutMs = 10;
    }

    @Data
    public static class Kafka {
        private int sendConcurrency = 4;
    }

    @Data
    public static class Metrics {
        private long intervalSec = 10;
    }
}
