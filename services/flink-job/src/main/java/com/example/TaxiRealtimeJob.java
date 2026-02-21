package com.example;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicLong;

public class TaxiRealtimeJob {

    // 늦게 도착해서 버린 이벤트(관측용)
    private static final OutputTag<TaxiEvent> LATE_EVENTS =
            new OutputTag<TaxiEvent>("late-events") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JobConfig jobConfig = loadConfigFromEnv();

        // ✅ 기본 parallelism=6 (env에 FLINK_PARALLELISM 있으면 덮어씀)
        env.setParallelism(jobConfig.parallelism);

        // ✅ Checkpointing (권장 기본값)
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE); // 30초

        CheckpointConfig ck = env.getCheckpointConfig();
        ck.setCheckpointTimeout(120_000);           
        ck.setMinPauseBetweenCheckpoints(10_000);     
        ck.setMaxConcurrentCheckpoints(1);             
        ck.setTolerableCheckpointFailureNumber(3); 
        ck.enableUnalignedCheckpoints();             
        ck.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        String bootstrap = System.getenv("FLINK_KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrap == null || bootstrap.isBlank()) {
            bootstrap = args.length > 0 ? args[0] : "kafka:29092";
        }

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
        String insertSql =
                "INSERT INTO " + clickhouseTable + " (trip_id, ts, zone_id, event) VALUES (?, ?, ?, ?)";
        String predictionTable = sanitizeIdentifier(jobConfig.clickhousePredictionTable, "taxi_predictions");
        String predictionInsertSql =
                "INSERT INTO " + predictionTable + " (prediction_time, target_time, zone_id, predicted_demand, model_version) VALUES (?, ?, ?, ?, ?)";

        System.out.printf(
                "[CONFIG] parallelism=%d, sinkParallelism=%d, topic=%s, groupId=%s, bootstrap=%s, clickhouseUrl=%s, sinkEnabled=%s, predictionSinkEnabled=%s, modelPath=%s, modelVersion=%s, outOfOrder=%ss, watermarkIdleness=%ss, idleCleanup=%smin%n",
                jobConfig.parallelism,
                jobConfig.clickhouseSinkParallelism,
                jobConfig.kafkaTopic,
                jobConfig.kafkaGroupId,
                bootstrap,
                chUrl,
                jobConfig.enableClickhouseSink,
                jobConfig.enablePredictionSink,
                jobConfig.onnxModelPath,
                jobConfig.modelVersion,
                jobConfig.watermarkOutOfOrdernessSec,
                jobConfig.watermarkIdlenessSec,
                jobConfig.idleCleanupMinutes
        );

        // 1) 안전 역직렬화
        DeserializationSchema<TaxiEvent> safeSchema = new SafeTaxiEventSchema();

        // 2) 워터마크 전략 (event.ts 기반)
        WatermarkStrategy<TaxiEvent> watermarkStrategy = WatermarkStrategy
                .<TaxiEvent>forBoundedOutOfOrderness(Duration.ofSeconds(jobConfig.watermarkOutOfOrdernessSec))
                .withTimestampAssigner((event, timestamp) -> parseTsOrMin(event))
                // Allow watermark progress when some source partitions go idle.
                .withIdleness(Duration.ofSeconds(jobConfig.watermarkIdlenessSec));

        KafkaSource<TaxiEvent> source = KafkaSource.<TaxiEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(jobConfig.kafkaTopic)
                .setGroupId(jobConfig.kafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(safeSchema)
                .build();

        // 3) 원본 스트림 (ts 파싱 검증 포함)
        DataStream<TaxiEvent> rawStream = env.fromSource(source, watermarkStrategy, "kafka-source")
                .filter(e -> {
                    if (e == null || e.ts == null || e.trip_id == null) return false;
                    try { Instant.parse(e.ts); return true; } catch (Exception ex) { return false; }
                });

        // ✅ 4) trip_id 단위 순서성 보장: 버퍼링+재정렬(event-time) + idle cleanup(processing-time)
        long idleTimeoutMs = Math.max(1, jobConfig.idleCleanupMinutes) * 60_000L;

        // ⚠️ Side output을 쓰려면 타입이 SingleOutputStreamOperator 여야 함
        SingleOutputStreamOperator<TaxiEvent> orderedPerTrip = rawStream
                .keyBy(e -> e.trip_id)
                .process(new PerTripEventTimeReorder(
                        jobConfig.watermarkOutOfOrdernessSec,
                        LATE_EVENTS,
                        idleTimeoutMs
                ));

        // 늦은 이벤트 모니터링
        // orderedPerTrip.getSideOutput(LATE_EVENTS)
        //         .map(e -> "[LATE_DROP] trip_id=" + e.trip_id + " ts=" + e.ts + " event=" + e.event)
        //         .print();

        // 5) 베이스 스트림 (가공)
        DataStream<TaxiEvent> baseStream = orderedPerTrip
                .map(new SpatialJoinFunction())
                .filter(e -> e.zone_id != null);

        // --- 트랙 1: ClickHouse 적재 ---
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
                            .withMaxRetries(5)
                            .build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl(chUrl)
                            .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                            .build()
            ))
            .setParallelism(jobConfig.clickhouseSinkParallelism);
        } else {
            System.out.println("[WARN] ClickHouse sink disabled by FLINK_ENABLE_CLICKHOUSE_SINK=false");
        }

        // --- 트랙 2: 3분 수요 집계 ---
        SingleOutputStreamOperator<DemandRow> demandRows = baseStream.filter(e -> "PICKUP".equals(normType(e.event)))
                .keyBy(e -> e.zone_id)
                .window(TumblingEventTimeWindows.of(Time.minutes(3)))
                .aggregate(new DemandAggregator(), new DemandWindowFn());

        demandRows.print();

        // --- 트랙 3: ONNX 추론 + 예측 적재 ---
        if (jobConfig.enablePredictionSink) {
            DataStream<PredictionRow> predictionRows = demandRows
                    .keyBy(d -> d.zone_id)
                    .process(new OnnxPredictionProcessFunction(
                            jobConfig.onnxModelPath,
                            jobConfig.modelVersion,
                            jobConfig.modelFeatureLagSteps,
                            jobConfig.modelHorizonSteps,
                            jobConfig.modelIntervalMinutes
                    ));

            predictionRows.addSink(JdbcSink.sink(
                    predictionInsertSql,
                    (ps, row) -> {
                        ps.setTimestamp(1, Timestamp.from(Instant.parse(row.prediction_time)));
                        ps.setTimestamp(2, Timestamp.from(Instant.parse(row.target_time)));
                        ps.setInt(3, row.zone_id);
                        ps.setFloat(4, row.predicted_demand);
                        ps.setString(5, row.model_version);
                    },
                    JdbcExecutionOptions.builder()
                            .withBatchSize(jobConfig.jdbcBatchSize)
                            .withBatchIntervalMs(jobConfig.jdbcBatchIntervalMs)
                            .withMaxRetries(5)
                            .build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl(chUrl)
                            .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                            .build()
            )).setParallelism(jobConfig.clickhouseSinkParallelism);
        } else {
            System.out.println("[WARN] Prediction sink disabled by FLINK_ENABLE_PREDICTION_SINK=false");
        }

        env.execute("Taxi Reliable Job (Ordered + Cleanup)");
    }

    /**
     * ✅ [핵심] trip_id(per-key) 단위 event-time 재정렬 + late drop + idle cleanup
     */
    public static class PerTripEventTimeReorder extends KeyedProcessFunction<Long, TaxiEvent, TaxiEvent> {
        private final long latenessMs;
        private final OutputTag<TaxiEvent> lateTag;
        private final long idleTimeoutMs;

        private transient ListState<TaxiEvent> bufferState;
        private transient ValueState<Long> lastEmittedTs;

        private transient ValueState<Long> lastSeenProcTime;
        private transient ValueState<Long> cleanupTimerProcTs;

        public PerTripEventTimeReorder(int outOfOrdernessSec,
                                      OutputTag<TaxiEvent> lateTag,
                                      long idleTimeoutMs) {
            this.latenessMs = Math.max(0, outOfOrdernessSec) * 1000L;
            this.lateTag = lateTag;
            this.idleTimeoutMs = idleTimeoutMs;
        }

        @Override
        public void open(Configuration parameters) {
            bufferState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("reorder-buffer", TaxiEvent.class)
            );
            lastEmittedTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-emitted-ts", Long.class)
            );

            lastSeenProcTime = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-seen-proc-time", Long.class)
            );
            cleanupTimerProcTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("cleanup-timer-proc-ts", Long.class)
            );
        }

        @Override
        public void processElement(TaxiEvent e, Context ctx, Collector<TaxiEvent> out) throws Exception {
            long eventTs = parseTsOrMin(e);
            if (eventTs == Long.MIN_VALUE) {
                ctx.output(lateTag, e);
                return;
            }

            long wm = ctx.timerService().currentWatermark();

            // watermark보다 과거면 이미 late → drop
            if (wm != Long.MIN_VALUE && eventTs <= wm) {
                ctx.output(lateTag, e);
                return;
            }

            // 버퍼 적재
            bufferState.add(e);

            // event-time flush 시도
            ctx.timerService().registerEventTimeTimer(eventTs + latenessMs);

            // ✅ idle cleanup 타이머 갱신 (processing time)
            long now = ctx.timerService().currentProcessingTime();
            lastSeenProcTime.update(now);

            Long prevCleanup = cleanupTimerProcTs.value();
            long nextCleanup = now + idleTimeoutMs;

            if (prevCleanup != null) {
                ctx.timerService().deleteProcessingTimeTimer(prevCleanup);
            }
            ctx.timerService().registerProcessingTimeTimer(nextCleanup);
            cleanupTimerProcTs.update(nextCleanup);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<TaxiEvent> out) throws Exception {
            // 1) processing-time cleanup
            Long cleanupTs = cleanupTimerProcTs.value();
            if (cleanupTs != null && timestamp == cleanupTs) {
                Long lastSeen = lastSeenProcTime.value();
                long now = ctx.timerService().currentProcessingTime();

                if (lastSeen == null || now - lastSeen >= idleTimeoutMs - 5) {
                    bufferState.clear();
                    lastEmittedTs.clear();
                    lastSeenProcTime.clear();
                    cleanupTimerProcTs.clear();
                }
                return;
            }

            // 2) event-time flush
            long wm = ctx.timerService().currentWatermark();
            if (wm == Long.MIN_VALUE) return;

            Long last = lastEmittedTs.value();
            long lastTs = (last == null) ? Long.MIN_VALUE : last;

            ArrayList<TaxiEvent> all = new ArrayList<>();
            for (TaxiEvent e : bufferState.get()) all.add(e);
            if (all.isEmpty()) return;

            all.sort((a, b) -> Long.compare(parseTsOrMin(a), parseTsOrMin(b)));

            ArrayList<TaxiEvent> remain = new ArrayList<>();

            for (TaxiEvent e : all) {
                long ts = parseTsOrMin(e);

                if (ts <= wm) {
                    if (ts < lastTs) {
                        ctx.output(lateTag, e);
                    } else {
                        out.collect(e);
                        lastTs = ts;
                    }
                } else {
                    remain.add(e);
                }
            }

            bufferState.update(remain);
            lastEmittedTs.update(lastTs);
        }
    }

    private static long parseTsOrMin(TaxiEvent event) {
        if (event == null || event.ts == null) return Long.MIN_VALUE;
        try { return Instant.parse(event.ts).toEpochMilli(); }
        catch (Exception e) { return Long.MIN_VALUE; }
    }

    // ---------------------------
    // Config / Utils
    // ---------------------------
    private static class JobConfig {
        // ✅ 고정 기본값
        int parallelism = 12;
        int watermarkOutOfOrdernessSec = 5;
        int watermarkIdlenessSec = 30;
        int idleCleanupMinutes = 20;

        int jdbcBatchSize = 50000;
        int jdbcBatchIntervalMs = 3000;
        int clickhouseSinkParallelism = 12;

        String kafkaTopic = "taxi-event-data";
        String kafkaGroupId = "taxi-realtime-flink";

        String clickhouseDatabase = "default";
        String clickhouseTable = "taxi_events";
        String clickhousePredictionTable = "taxi_predictions";

        boolean enableClickhouseSink = true;
        boolean enablePredictionSink = true;

        String onnxModelPath = "/opt/flink/model/taxi_demand_model.onnx";
        String modelVersion = "onnx_v1";
        int modelFeatureLagSteps = 20;
        int modelHorizonSteps = 5;
        int modelIntervalMinutes = 3;
    }

    private static JobConfig loadConfigFromEnv() {
        JobConfig cfg = new JobConfig();

        // env가 있으면 덮어쓰기, 없으면 기본값 유지
        cfg.parallelism = getEnvInt("FLINK_PARALLELISM", cfg.parallelism);
        cfg.kafkaTopic = getEnvString("FLINK_KAFKA_TOPIC", cfg.kafkaTopic);
        cfg.kafkaGroupId = getEnvString("FLINK_KAFKA_GROUP_ID", cfg.kafkaGroupId);

        cfg.clickhouseDatabase = getEnvString("FLINK_CLICKHOUSE_DATABASE", cfg.clickhouseDatabase);
        cfg.clickhouseTable = getEnvString("FLINK_CLICKHOUSE_TABLE", cfg.clickhouseTable);
        cfg.clickhousePredictionTable = getEnvString("FLINK_CLICKHOUSE_PREDICTION_TABLE", cfg.clickhousePredictionTable);

        cfg.watermarkOutOfOrdernessSec = getEnvInt("FLINK_WATERMARK_OUT_OF_ORDERNESS_SEC", cfg.watermarkOutOfOrdernessSec);
        cfg.watermarkIdlenessSec = getEnvInt("FLINK_WATERMARK_IDLENESS_SEC", cfg.watermarkIdlenessSec);
        cfg.idleCleanupMinutes = getEnvInt("FLINK_IDLE_CLEANUP_MINUTES", cfg.idleCleanupMinutes);

        cfg.jdbcBatchSize = getEnvInt("FLINK_JDBC_BATCH_SIZE", cfg.jdbcBatchSize);
        cfg.jdbcBatchIntervalMs = getEnvInt("FLINK_JDBC_BATCH_INTERVAL_MS", cfg.jdbcBatchIntervalMs);
        cfg.clickhouseSinkParallelism = getEnvInt("FLINK_CLICKHOUSE_SINK_PARALLELISM", cfg.parallelism);
        if (cfg.clickhouseSinkParallelism <= 0) {
            cfg.clickhouseSinkParallelism = cfg.parallelism;
        }

        cfg.enableClickhouseSink = getEnvBoolean("FLINK_ENABLE_CLICKHOUSE_SINK", cfg.enableClickhouseSink);
        cfg.enablePredictionSink = getEnvBoolean("FLINK_ENABLE_PREDICTION_SINK", cfg.enablePredictionSink);

        cfg.onnxModelPath = getEnvString("FLINK_ONNX_MODEL_PATH", cfg.onnxModelPath);
        cfg.modelVersion = getEnvString("FLINK_MODEL_VERSION", cfg.modelVersion);
        cfg.modelFeatureLagSteps = getEnvInt("FLINK_MODEL_FEATURE_LAG_STEPS", cfg.modelFeatureLagSteps);
        cfg.modelHorizonSteps = getEnvInt("FLINK_MODEL_HORIZON_STEPS", cfg.modelHorizonSteps);
        cfg.modelIntervalMinutes = getEnvInt("FLINK_MODEL_INTERVAL_MINUTES", cfg.modelIntervalMinutes);
        return cfg;
    }

    private static int getEnvInt(String key, int def) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) return def;
        try { return Integer.parseInt(raw.trim()); } catch (Exception e) { return def; }
    }

    private static String getEnvString(String key, String def) {
        String raw = System.getenv(key);
        return (raw == null || raw.isBlank()) ? def : raw;
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

    // ---------------------------
    // Aggregation
    // ---------------------------
    public static class DemandRow {
        public int zone_id;
        public String window_end;
        public long count;

        public DemandRow(int z, String we, long c) {
            this.zone_id = z;
            this.window_end = we;
            this.count = c;
        }

        @Override
        public String toString() {
            return String.format("[ML_DEMAND] Zone: %d, Time: %s, Count: %d", zone_id, window_end, count);
        }
    }

    public static class DemandAggregator implements AggregateFunction<TaxiEvent, Long, Long> {
        @Override public Long createAccumulator() { return 0L; }
        @Override public Long add(TaxiEvent v, Long a) { return a + 1; }
        @Override public Long getResult(Long a) { return a; }
        @Override public Long merge(Long a, Long b) { return a + b; }
    }

    public static class DemandWindowFn extends ProcessWindowFunction<Long, DemandRow, Integer, TimeWindow> {
        @Override
        public void process(Integer z, Context c, Iterable<Long> e, Collector<DemandRow> o) {
            o.collect(new DemandRow(z, Instant.ofEpochMilli(c.window().getEnd()).toString(), e.iterator().next()));
        }
    }

    public static class PredictionRow {
        public String prediction_time;
        public String target_time;
        public int zone_id;
        public float predicted_demand;
        public String model_version;

        public PredictionRow() {}

        public PredictionRow(String predictionTime, String targetTime, int zoneId, float predictedDemand, String modelVersion) {
            this.prediction_time = predictionTime;
            this.target_time = targetTime;
            this.zone_id = zoneId;
            this.predicted_demand = predictedDemand;
            this.model_version = modelVersion;
        }
    }

    public static class OnnxPredictionProcessFunction extends KeyedProcessFunction<Integer, DemandRow, PredictionRow> {
        private final String modelPath;
        private final String modelVersion;
        private final int featureLagSteps;
        private final int horizonSteps;
        private final int intervalMinutes;

        private transient ListState<Long> historyState;
        private transient OrtEnvironment ortEnv;
        private transient OrtSession ortSession;
        private transient String onnxInputName;

        public OnnxPredictionProcessFunction(
                String modelPath,
                String modelVersion,
                int featureLagSteps,
                int horizonSteps,
                int intervalMinutes
        ) {
            this.modelPath = modelPath;
            this.modelVersion = modelVersion;
            this.featureLagSteps = Math.max(1, featureLagSteps);
            this.horizonSteps = Math.max(1, horizonSteps);
            this.intervalMinutes = Math.max(1, intervalMinutes);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            historyState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("zone-demand-history", Long.class)
            );

            try {
                ortEnv = OrtEnvironment.getEnvironment();
                OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
                ortSession = ortEnv.createSession(modelPath, opts);
                onnxInputName = ortSession.getInputNames().iterator().next();
                System.out.printf("[ONNX] model loaded path=%s input=%s%n", modelPath, onnxInputName);
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize ONNX session: " + modelPath, e);
            }
        }

        @Override
        public void close() throws Exception {
            if (ortSession != null) {
                ortSession.close();
            }
        }

        @Override
        public void processElement(DemandRow value, Context ctx, Collector<PredictionRow> out) throws Exception {
            if (value == null || value.zone_id <= 0 || value.window_end == null) {
                return;
            }

            List<Long> history = new ArrayList<>();
            for (Long v : historyState.get()) {
                if (v != null) {
                    history.add(v);
                }
            }

            if (history.size() >= featureLagSteps) {
                long lagVal = history.get(history.size() - featureLagSteps);
                PredictionRow row = predict(value, lagVal);
                if (row != null) {
                    out.collect(row);
                }
            }

            history.add(value.count);
            int keepN = Math.max(256, featureLagSteps + horizonSteps + 16);
            if (history.size() > keepN) {
                history = history.subList(history.size() - keepN, history.size());
            }
            historyState.update(history);
        }

        private PredictionRow predict(DemandRow value, long lag20) throws OrtException {
            Instant predictionTs = Instant.parse(value.window_end);
            Instant targetTs = predictionTs.plusSeconds((long) horizonSteps * intervalMinutes * 60L);
            ZonedDateTime dt = predictionTs.atZone(ZoneOffset.UTC);
            int hour = dt.getHour();
            int dayOfWeek = dt.getDayOfWeek().getValue() - 1; // Monday=0 ... Sunday=6
            int isWeekend = dayOfWeek >= 5 ? 1 : 0;

            float[][] input = new float[][]{
                    {
                            (float) value.zone_id,
                            (float) hour,
                            (float) dayOfWeek,
                            (float) isWeekend,
                            (float) lag20
                    }
            };

            try (OnnxTensor tensor = OnnxTensor.createTensor(ortEnv, input);
                 OrtSession.Result result = ortSession.run(Map.of(onnxInputName, tensor))) {
                Object raw = result.get(0).getValue();
                float pred = extractPrediction(raw);
                if (pred < 0f) {
                    pred = 0f;
                }
                return new PredictionRow(
                        predictionTs.toString(),
                        targetTs.toString(),
                        value.zone_id,
                        pred,
                        modelVersion
                );
            }
        }

        private float extractPrediction(Object raw) {
            if (raw instanceof float[][]) {
                float[][] a = (float[][]) raw;
                return (a.length > 0 && a[0].length > 0) ? a[0][0] : 0f;
            }
            if (raw instanceof float[]) {
                float[] a = (float[]) raw;
                return a.length > 0 ? a[0] : 0f;
            }
            if (raw instanceof double[][]) {
                double[][] a = (double[][]) raw;
                return (a.length > 0 && a[0].length > 0) ? (float) a[0][0] : 0f;
            }
            if (raw instanceof double[]) {
                double[] a = (double[]) raw;
                return a.length > 0 ? (float) a[0] : 0f;
            }
            throw new IllegalStateException("Unsupported ONNX output type: " + raw.getClass());
        }
    }

    // ---------------------------
    // Safe Deserialization
    // ---------------------------
    public static class SafeTaxiEventSchema implements DeserializationSchema<TaxiEvent> {
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper objectMapper;
        private transient AtomicLong errorCount;

        @Override
        public void open(InitializationContext context) {
            objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            errorCount = new AtomicLong(0);
            System.out.println("[SafeTaxiEventSchema] Initialized");
        }

        @Override
        public TaxiEvent deserialize(byte[] message) throws IOException {
            if (message == null || message.length == 0) return null;
            try {
                return objectMapper.readValue(message, TaxiEvent.class);
            } catch (Exception e) {
                long count = errorCount.incrementAndGet();
                if (count <= 5 || count % 10000 == 0) {
                    String sample = new String(message, 0, Math.min(message.length, 200));
                    System.err.printf("[DESER_ERROR] #%d: %s | sample: %s%n", count, e.getMessage(), sample);
                }
                return null;
            }
        }

        @Override public boolean isEndOfStream(TaxiEvent nextElement) { return false; }

        @Override
        public TypeInformation<TaxiEvent> getProducedType() {
            return TypeInformation.of(TaxiEvent.class);
        }
    }
}
