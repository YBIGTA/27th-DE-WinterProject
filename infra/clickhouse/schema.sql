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
