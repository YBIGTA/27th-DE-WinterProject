package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public class TaxiRealtimeJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String configPath = System.getenv("FLINK_CONFIG_PATH");
        if (configPath == null || configPath.isBlank()) {
            configPath = args.length > 2 ? args[2] : "config/default.yaml";
        }
        JobConfig jobConfig = loadConfig(configPath);

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

        // 1. 안전한 역직렬화
        DeserializationSchema<TaxiEvent> safeSchema = new JsonDeserializationSchema<TaxiEvent>(TaxiEvent.class) {
            @Override
            public TaxiEvent deserialize(byte[] message) throws IOException {
                if (message == null || message.length == 0) return null;
                try { return super.deserialize(message); } catch (Exception e) { return null; }
            }
        };

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
        baseStream.addSink(JdbcSink.sink(
                insertSql,
                (ps, event) -> {
                    ps.setLong(1, event.trip_id);
                    ps.setString(2, event.ts);
                    ps.setInt(3, event.zone_id);
                    ps.setString(4, normType(event.event));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(jobConfig.jdbcBatchSize)
                        .withBatchIntervalMs(jobConfig.jdbcBatchIntervalMs)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(chUrl).withDriverName("com.clickhouse.jdbc.ClickHouseDriver").build()
        ));

        // --- 트랙 2: 3분 수요 집계 (ML용) ---
        baseStream.filter(e -> "PICKUP".equals(normType(e.event)))
                .keyBy(e -> e.zone_id)
                .window(TumblingEventTimeWindows.of(Time.minutes(jobConfig.windowDemandMinutes)))
                .aggregate(new DemandAggregator(), new DemandWindowFn())
                .print(); 

        env.execute("Taxi Reliable Job (V13.1-Release)");
    }

    private static class JobConfig {
        int parallelism = 1;
        int watermarkOutOfOrdernessSec = 5;
        int windowDemandMinutes = 3;
        int jdbcBatchSize = 5000;
        int jdbcBatchIntervalMs = 5000;
        String kafkaTopic = "taxi-event-data";
        String clickhouseDatabase = "default";
        String clickhouseTable = "taxi_events";
        int taskmanagerSlots = 2;
    }

    @SuppressWarnings("unchecked")
    private static JobConfig loadConfig(String path) {
        JobConfig cfg = new JobConfig();
        if (path == null || path.isBlank()) return cfg;
        try (InputStream in = Files.newInputStream(Path.of(path))) {
            Object data = new Yaml().load(in);
            if (!(data instanceof Map)) return cfg;
            Map<String, Object> root = (Map<String, Object>) data;

            cfg.parallelism = getInt(root, "parallelism", cfg.parallelism);
            cfg.kafkaTopic = getString(getMap(root, "kafka"), "topic", cfg.kafkaTopic);
            cfg.taskmanagerSlots = getInt(getMap(root, "taskmanager"), "slots", cfg.taskmanagerSlots);

            Map<String, Object> clickhouse = getMap(root, "clickhouse");
            cfg.clickhouseDatabase = getString(clickhouse, "database", cfg.clickhouseDatabase);
            cfg.clickhouseTable = getString(clickhouse, "table", cfg.clickhouseTable);

            Map<String, Object> watermark = getMap(root, "watermark");
            cfg.watermarkOutOfOrdernessSec = getInt(watermark, "out_of_orderness_sec", cfg.watermarkOutOfOrdernessSec);

            Map<String, Object> window = getMap(root, "window");
            cfg.windowDemandMinutes = getInt(window, "demand_minutes", cfg.windowDemandMinutes);

            Map<String, Object> jdbc = getMap(root, "jdbc");
            cfg.jdbcBatchSize = getInt(jdbc, "batch_size", cfg.jdbcBatchSize);
            cfg.jdbcBatchIntervalMs = getInt(jdbc, "batch_interval_ms", cfg.jdbcBatchIntervalMs);
        } catch (Exception e) {
            System.err.println("[Config] Using defaults (file not found: " + path + ")");
        }
        return cfg;
    }

    private static Map<String, Object> getMap(Map<String, Object> root, String key) {
        if (root == null) return null;
        Object val = root.get(key);
        if (val instanceof Map) return (Map<String, Object>) val;
        return null;
    }

    private static int getInt(Map<String, Object> map, String key, int def) {
        if (map == null || key == null) return def;
        Object val = map.get(key);
        if (val instanceof Number) return ((Number) val).intValue();
        if (val instanceof String) {
            try { return Integer.parseInt((String) val); } catch (Exception ignored) {}
        }
        return def;
    }

    private static String getString(Map<String, Object> map, String key, String def) {
        if (map == null || key == null) return def;
        Object val = map.get(key);
        if (val instanceof String) return (String) val;
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
}
