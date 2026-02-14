CREATE TABLE IF NOT EXISTS default.taxi_events
(
    `trip_id` UInt64,
    `ts` DateTime,
    `zone_id` UInt32,
    `event` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
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
