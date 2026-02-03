CREATE TABLE IF NOT EXISTS default.taxi_events
(
    `trip_id` UInt64,
    `ts` DateTime,
    `zone_id` UInt32,
    `event` String
)
ENGINE = MergeTree
ORDER BY (trip_id, ts);
