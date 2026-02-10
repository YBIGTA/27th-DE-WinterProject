package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public class TaxiRealtimeJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JobConfig jobConfig = loadConfigFromEnv();

        env.setParallelism(jobConfig.parallelism);

        String bootstrap = System.getenv("FLINK_KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrap == null || bootstrap.isBlank()) {
            bootstrap = args.length > 0 ? args[0] : "kafka:29092";
        }
        // ✅ Tailscale 연결 시 이 주소를 Tailscale IP로 변경하면 됩니다.
        String chUrl = System.getenv("FLINK_CLICKHOUSE_URL");
        if (chUrl == null || chUrl.isBlank()) {
            String chHost = System.getenv("FLINK_CLICKHOUSE_HOST");
            String chPort = System.getenv("FLINK_CLICKHOUSE_PORT");
            String chDb = jobConfig.clickhouseDatabase;
            if (chHost != null && !chHost.isBlank()) {
                String port = (chPort == null || chPort.isBlank()) ? "8123" : chPort;
                String db = (chDb == null || chDb.isBlank()) ? "default" : chDb;
                chUrl = "jdbc:clickhouse://" + chHost + ":" + port + "/" + db;
            } else {
                String fallbackDb = (chDb == null || chDb.isBlank()) ? "default" : chDb;
                chUrl = args.length > 1 ? args[1] : "jdbc:clickhouse://clickhouse:8123/" + fallbackDb;
            }
        }
        String clickhouseTable = sanitizeIdentifier(jobConfig.clickhouseTable, "taxi_events");
        String insertSql = "INSERT INTO " + clickhouseTable + " (trip_id, ts, zone_id, event) VALUES (?, ?, ?, ?)";

        System.out.printf(
                "[CONFIG] topic=%s, groupId=%s, bootstrap=%s, clickhouseUrl=%s, sinkEnabled=%s%n",
                jobConfig.kafkaTopic, jobConfig.kafkaGroupId, bootstrap, chUrl, jobConfig.enableClickhouseSink
        );

        // 1. 안전한 역직렬화 (Jackson ObjectMapper 직접 사용)
        DeserializationSchema<TaxiEvent> safeSchema = new SafeTaxiEventSchema();

        // 2. 워터마크 전략 (NPE 방어)
        WatermarkStrategy<TaxiEvent> watermarkStrategy = WatermarkStrategy
                .<TaxiEvent>forBoundedOutOfOrderness(Duration.ofSeconds(jobConfig.watermarkOutOfOrdernessSec))
                .withTimestampAssigner((event, timestamp) -> {
                    if (event == null || event.ts == null) return Long.MIN_VALUE;
                    try { return Instant.parse(event.ts).toEpochMilli(); }
                    catch (Exception e) { return Long.MIN_VALUE; }
                });

        KafkaSource<TaxiEvent> source = KafkaSource.<TaxiEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(jobConfig.kafkaTopic)
                .setGroupId(jobConfig.kafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(safeSchema)
                .build();

        // 3. 베이스 스트림 (가공 로직)
        DataStream<TaxiEvent> baseStream = env.fromSource(source, watermarkStrategy, "kafka-source")
                .filter(e -> {
                    if (e == null || e.ts == null || e.trip_id == null) return false;
                    try { Instant.parse(e.ts); return true; } catch (Exception ex) { return false; }
                })
                .map(new SpatialJoinFunction())
                .filter(e -> e.zone_id != null);

        // --- 트랙 1: 실시간 공급 (ClickHouse Direct Ingest) ---
        if (jobConfig.enableClickhouseSink) {
            baseStream.addSink(JdbcSink.sink(
                    insertSql,
                    (ps, event) -> {
                        ps.setLong(1, event.trip_id);
                        ps.setTimestamp(2, Timestamp.from(Instant.parse(event.ts)));
                        ps.setInt(3, event.zone_id);
                        ps.setString(4, normType(event.event));
                    },
                    JdbcExecutionOptions.builder()
                            .withBatchSize(jobConfig.jdbcBatchSize)
                            .withBatchIntervalMs(jobConfig.jdbcBatchIntervalMs)
                            .build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(chUrl).withDriverName("com.clickhouse.jdbc.ClickHouseDriver").build()
            ));
        } else {
            System.out.println("[WARN] ClickHouse sink disabled by FLINK_ENABLE_CLICKHOUSE_SINK=false");
        }

        // --- 트랙 2: 3분 수요 집계 (ML용) ---
        baseStream.filter(e -> "PICKUP".equals(normType(e.event)))
                .keyBy(e -> e.zone_id)
                .window(TumblingEventTimeWindows.of(Time.minutes(jobConfig.windowDemandMinutes)))
                .aggregate(new DemandAggregator(), new DemandWindowFn())
                .print(); 

        env.execute("Taxi Reliable Job (V13.1-Release)");
    }

    private static class JobConfig {
        int parallelism = 3;
        int watermarkOutOfOrdernessSec = 5;
        int windowDemandMinutes = 3;
        int jdbcBatchSize = 5000;
        int jdbcBatchIntervalMs = 5000;
        String kafkaTopic = "taxi-event-data";
        String kafkaGroupId = "taxi-realtime-flink";
        String clickhouseDatabase = "default";
        String clickhouseTable = "taxi_events";
        int taskmanagerSlots = 2;
        boolean enableClickhouseSink = true;
    }

    private static JobConfig loadConfigFromEnv() {
        JobConfig cfg = new JobConfig();
        cfg.parallelism = getEnvInt("FLINK_PARALLELISM", cfg.parallelism);
        cfg.kafkaTopic = getEnvString("FLINK_KAFKA_TOPIC", cfg.kafkaTopic);
        cfg.kafkaGroupId = getEnvString("FLINK_KAFKA_GROUP_ID", cfg.kafkaGroupId);
        cfg.taskmanagerSlots = getEnvInt("FLINK_TASKMANAGER_SLOTS", cfg.taskmanagerSlots);
        cfg.clickhouseDatabase = getEnvString("FLINK_CLICKHOUSE_DATABASE", cfg.clickhouseDatabase);
        cfg.clickhouseTable = getEnvString("FLINK_CLICKHOUSE_TABLE", cfg.clickhouseTable);
        cfg.watermarkOutOfOrdernessSec = getEnvInt("FLINK_WATERMARK_OUT_OF_ORDERNESS_SEC", cfg.watermarkOutOfOrdernessSec);
        cfg.windowDemandMinutes = getEnvInt("FLINK_WINDOW_DEMAND_MINUTES", cfg.windowDemandMinutes);
        cfg.jdbcBatchSize = getEnvInt("FLINK_JDBC_BATCH_SIZE", cfg.jdbcBatchSize);
        cfg.jdbcBatchIntervalMs = getEnvInt("FLINK_JDBC_BATCH_INTERVAL_MS", cfg.jdbcBatchIntervalMs);
        cfg.enableClickhouseSink = getEnvBoolean("FLINK_ENABLE_CLICKHOUSE_SINK", cfg.enableClickhouseSink);
        return cfg;
    }

    private static int getEnvInt(String key, int def) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) return def;
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ignored) {
            return def;
        }
    }

    private static String getEnvString(String key, String def) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) return def;
        return raw;
    }

    private static boolean getEnvBoolean(String key, boolean def) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) return def;
        if ("true".equalsIgnoreCase(raw) || "1".equals(raw) || "yes".equalsIgnoreCase(raw)) return true;
        if ("false".equalsIgnoreCase(raw) || "0".equals(raw) || "no".equalsIgnoreCase(raw)) return false;
        return def;
    }

    private static String sanitizeIdentifier(String value, String def) {
        if (value == null || value.isBlank()) return def;
        return value.matches("[A-Za-z_][A-Za-z0-9_]*") ? value : def;
    }

    public static String normType(String s) {
        return s == null ? "" : s.trim().toUpperCase().replace("_", "").replace(" ", "");
    }

    // --- DTO 및 함수들 ---
    public static class DemandRow {
        public int zone_id; public String window_end; public long count;
        public DemandRow(int z, String we, long c) { this.zone_id = z; this.window_end = we; this.count = c; }
        @Override public String toString() { return String.format("[ML_DEMAND] Zone: %d, Time: %s, Count: %d", zone_id, window_end, count); }
    }

    public static class DemandAggregator implements AggregateFunction<TaxiEvent, Long, Long> {
        @Override public Long createAccumulator() { return 0L; }
        @Override public Long add(TaxiEvent v, Long a) { return a + 1; }
        @Override public Long getResult(Long a) { return a; }
        @Override public Long merge(Long a, Long b) { return a + b; }
    }

    // ✅ ProcessWindowFunction의 내부 Context를 사용하도록 수정
    public static class DemandWindowFn extends ProcessWindowFunction<Long, DemandRow, Integer, TimeWindow> {
        @Override
        public void process(Integer z, Context c, Iterable<Long> e, Collector<DemandRow> o) {
            o.collect(new DemandRow(z, Instant.ofEpochMilli(c.window().getEnd()).toString(), e.iterator().next()));
        }
    }

    // Jackson ObjectMapper 기반 역직렬화 (JsonDeserializationSchema 대신 직접 구현)
    public static class SafeTaxiEventSchema implements DeserializationSchema<TaxiEvent> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper objectMapper;
        private transient AtomicLong errorCount;

        @Override
        public void open(InitializationContext context) {
            objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            errorCount = new AtomicLong(0);
            System.out.println("[SafeTaxiEventSchema] Initialized with ObjectMapper");
        }

        private ObjectMapper getMapper() {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                errorCount = new AtomicLong(0);
                System.out.println("[SafeTaxiEventSchema] Lazy-initialized ObjectMapper");
            }
            return objectMapper;
        }

        @Override
        public TaxiEvent deserialize(byte[] message) throws IOException {
            if (message == null || message.length == 0) return null;
            try {
                return getMapper().readValue(message, TaxiEvent.class);
            } catch (Exception e) {
                long count = errorCount.incrementAndGet();
                if (count <= 5 || count % 10000 == 0) {
                    String sample = new String(message, 0, Math.min(message.length, 200));
                    System.err.printf("[DESER_ERROR] #%d: %s | sample: %s%n", count, e.getMessage(), sample);
                }
                return null;
            }
        }

        @Override
        public boolean isEndOfStream(TaxiEvent nextElement) {
            return false;
        }

        @Override
        public TypeInformation<TaxiEvent> getProducedType() {
            return TypeInformation.of(TaxiEvent.class);
        }
    }
}
