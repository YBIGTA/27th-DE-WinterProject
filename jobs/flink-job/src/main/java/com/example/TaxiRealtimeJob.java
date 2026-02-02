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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class TaxiRealtimeJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String bootstrap = args.length > 0 ? args[0] : "kafka:29092";
        // ✅ Tailscale 연결 시 이 주소를 Tailscale IP로 변경하면 됩니다.
        String chUrl = args.length > 1 ? args[1] : "jdbc:clickhouse://clickhouse:8123/default";

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
                .<TaxiEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> {
                    if (event == null || event.ts == null) return Long.MIN_VALUE;
                    try { return Instant.parse(event.ts).toEpochMilli(); }
                    catch (Exception e) { return Long.MIN_VALUE; }
                });

        KafkaSource<TaxiEvent> source = KafkaSource.<TaxiEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics("taxi_raw_events")
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
                "INSERT INTO taxi_raw_data (trip_id, ts, zone_id, event) VALUES (?, ?, ?, ?)",
                (ps, event) -> {
                    ps.setLong(1, event.trip_id);
                    ps.setString(2, event.ts);
                    ps.setInt(3, event.zone_id);
                    ps.setString(4, normType(event.event));
                },
                JdbcExecutionOptions.builder().withBatchSize(5000).withBatchIntervalMs(5000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(chUrl).withDriverName("com.clickhouse.jdbc.ClickHouseDriver").build()
        ));

        // --- 트랙 2: 3분 수요 집계 (ML용) ---
        baseStream.filter(e -> "PICKUP".equals(normType(e.event)))
                .keyBy(e -> e.zone_id)
                .window(TumblingEventTimeWindows.of(Time.minutes(3)))
                .aggregate(new DemandAggregator(), new DemandWindowFn())
                .print(); 

        env.execute("Taxi Reliable Job (V13.1-Release)");
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