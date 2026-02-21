CREATE TABLE IF NOT EXISTS default.taxi_events
(
    `trip_id` UInt64,
    `ts` DateTime,
    `zone_id` UInt32,
    `event` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (ts, zone_id, trip_id);

CREATE TABLE IF NOT EXISTS default.taxi_zones
(
    `zone_id` UInt32,
    `zone_name` String,
    `borough` String,
    `lat` Float64,
    `lon` Float64
)
ENGINE = MergeTree
ORDER BY zone_id;

CREATE TABLE IF NOT EXISTS default.taxi_predictions
(
    `prediction_time` DateTime,
    `target_time` DateTime,
    `zone_id` UInt32,
    `predicted_demand` Float32,
    `model_version` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(prediction_time)
ORDER BY (prediction_time, zone_id, model_version);

-- Dedup serving layer for at-least-once JDBC sink replay handling.
-- Raw sink targets (taxi_events / taxi_predictions) remain unchanged.
CREATE TABLE IF NOT EXISTS default.taxi_events_serving
(
    `trip_id` UInt64,
    `ts` DateTime,
    `zone_id` UInt32,
    `event` String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (trip_id, ts, zone_id, event);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_taxi_events_to_serving
TO default.taxi_events_serving
AS
SELECT trip_id, ts, zone_id, event
FROM default.taxi_events;

CREATE VIEW IF NOT EXISTS default.taxi_events_latest
AS
SELECT trip_id, ts, zone_id, event
FROM default.taxi_events_serving FINAL;

CREATE TABLE IF NOT EXISTS default.taxi_predictions_serving
(
    `prediction_time` DateTime,
    `target_time` DateTime,
    `zone_id` UInt32,
    `predicted_demand` Float32,
    `model_version` String
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(prediction_time)
ORDER BY (prediction_time, target_time, zone_id, model_version);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_taxi_predictions_to_serving
TO default.taxi_predictions_serving
AS
SELECT prediction_time, target_time, zone_id, predicted_demand, model_version
FROM default.taxi_predictions;

CREATE VIEW IF NOT EXISTS default.taxi_predictions_latest
AS
SELECT prediction_time, target_time, zone_id, predicted_demand, model_version
FROM default.taxi_predictions_serving FINAL;
