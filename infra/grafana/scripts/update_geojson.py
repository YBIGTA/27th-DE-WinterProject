#!/usr/bin/env python3
"""
Periodically query ClickHouse for pickup demand per zone
and regenerate tier-based GeoJSON files for Grafana geomap.
"""

import json
import os
import time
import urllib.request
import urllib.parse

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
MASTER_GEOJSON = os.environ.get("MASTER_GEOJSON", "/data/geojson/taxi_zones.geojson")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/data/geojson")
INTERVAL = int(os.environ.get("UPDATE_INTERVAL", "10"))

QUERY = """
SELECT zone_id, count() as demand
FROM default.taxi_events
WHERE event = 'PICKUP'
GROUP BY zone_id
FORMAT JSONEachRow
"""

TIERS = [
    ("t0_none",    0,     0),
    ("t1_minimal", 1,     10),
    ("t2_low",     11,    50),
    ("t3_medium",  51,    200),
    ("t4_mid",     201,   800),
    ("t5_high",    801,   2000),
    ("t6_vhigh",   2001,  5000),
    ("t7_max",     5001,  float("inf")),
]


def query_clickhouse():
    """Query ClickHouse and return {zone_id: demand} dict."""
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    data = QUERY.strip().encode()
    try:
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "text/plain"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            lines = resp.read().decode().strip().split("\n")
            demand = {}
            for line in lines:
                if not line:
                    continue
                row = json.loads(line)
                demand[int(row["zone_id"])] = int(row["demand"])
            return demand
    except Exception as e:
        print(f"[WARN] ClickHouse query failed: {e}")
        return {}


def classify_tier(demand_value):
    """Return tier index based on demand value."""
    for i, (_, lo, hi) in enumerate(TIERS):
        if lo <= demand_value <= hi:
            return i
    return 0


def load_master_geojson():
    with open(MASTER_GEOJSON) as f:
        return json.load(f)


def generate_tier_files(master, demand_map):
    """Split master GeoJSON features into 8 tier files based on demand."""
    tier_features = {i: [] for i in range(len(TIERS))}

    for feature in master["features"]:
        zone_id = feature["properties"].get("zone_id")
        d = demand_map.get(zone_id, 0)
        feature["properties"]["demand"] = d
        tier_idx = classify_tier(d)
        tier_features[tier_idx].append(feature)

    for i, (tier_name, _, _) in enumerate(TIERS):
        out = {
            "type": "FeatureCollection",
            "features": tier_features[i],
        }
        path = os.path.join(OUTPUT_DIR, f"taxi_zones_{tier_name}.geojson")
        with open(path, "w") as f:
            json.dump(out, f, separators=(",", ":"))

    counts = {TIERS[i][0]: len(tier_features[i]) for i in range(len(TIERS))}
    print(f"[INFO] Updated GeoJSON: {counts}", flush=True)


def main():
    print(f"[INFO] Starting GeoJSON updater (interval={INTERVAL}s)", flush=True)
    print(f"[INFO] ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}", flush=True)
    print(f"[INFO] Master GeoJSON: {MASTER_GEOJSON}", flush=True)
    print(f"[INFO] Output dir: {OUTPUT_DIR}", flush=True)

    master = load_master_geojson()
    print(f"[INFO] Loaded {len(master['features'])} zones from master GeoJSON", flush=True)

    # Generate initial empty state
    generate_tier_files(master, {})

    while True:
        demand_map = query_clickhouse()
        generate_tier_files(master, demand_map)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
