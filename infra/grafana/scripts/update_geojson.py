#!/usr/bin/env python3
"""
Periodically query ClickHouse for pickup demand per zone
and regenerate tier-based GeoJSON files for Grafana geomap.
"""

import json
import os
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from functools import partial
import urllib.request
import urllib.parse

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
MASTER_GEOJSON = os.environ.get("MASTER_GEOJSON", "/data/geojson/taxi_zones.geojson")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/data/geojson")
INTERVAL = int(os.environ.get("UPDATE_INTERVAL", "10"))
HTTP_PORT = int(os.environ.get("GEOJSON_HTTP_PORT", "18081"))
LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES", "1440"))

QUERY_TEMPLATE = """
SELECT zone_id, count() as demand
FROM default.taxi_events
WHERE event = 'PICKUP'
  AND ts >= (SELECT max(ts) - toIntervalMinute({lookback_minutes}) FROM default.taxi_events)
GROUP BY zone_id
FORMAT JSONEachRow
"""

# Percentage-of-total demand tiers over the latest 1h window.
# Values are demand share in percent, e.g. 0.15 means 0.15%.
TIERS = [
    ("t0_none",    0.0,    0.0),
    ("t1_minimal", 0.0,    0.05),
    ("t2_low",     0.05,   0.15),
    ("t3_medium",  0.15,   0.40),
    ("t4_mid",     0.40,   0.80),
    ("t5_high",    0.80,   1.50),
    ("t6_vhigh",   1.50,   3.00),
    ("t7_max",     3.00,   float("inf")),
]

class NoCacheGeoJsonHandler(SimpleHTTPRequestHandler):
    """Serve generated GeoJSON files without browser caching."""

    def end_headers(self):
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()

    def log_message(self, format, *args):
        # Keep logs compact; updater prints its own periodic status.
        pass


def start_geojson_server():
    handler = partial(NoCacheGeoJsonHandler, directory=OUTPUT_DIR)
    server = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"[INFO] GeoJSON HTTP server started on :{HTTP_PORT} (no-cache)", flush=True)


def query_clickhouse():
    """Query ClickHouse and return {zone_id: demand} dict."""
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    data = QUERY_TEMPLATE.format(lookback_minutes=LOOKBACK_MINUTES).strip().encode()
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


def classify_tier_by_pct(demand_pct):
    """Return tier index based on demand share percentage."""
    if demand_pct <= 0.0:
        return 0
    for i, (_, lo, hi) in enumerate(TIERS):
        if i == 0:
            continue
        if lo < demand_pct <= hi:
            return i
    return 0


def load_master_geojson():
    with open(MASTER_GEOJSON) as f:
        return json.load(f)


def generate_tier_files(master, demand_map):
    """Split master GeoJSON features into 8 tier files based on demand."""
    tier_features = {i: [] for i in range(len(TIERS))}
    total_demand = sum(demand_map.values())

    for feature in master["features"]:
        zone_id = feature["properties"].get("zone_id")
        d = demand_map.get(zone_id, 0)
        d_pct = (float(d) * 100.0 / float(total_demand)) if total_demand > 0 else 0.0
        feature["properties"]["demand"] = d
        feature["properties"]["demand_pct"] = round(d_pct, 4)
        tier_idx = classify_tier_by_pct(d_pct)
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
    print(f"[INFO] Updated GeoJSON: {counts} total_pickups={total_demand}", flush=True)


def main():
    print(f"[INFO] Starting GeoJSON updater (interval={INTERVAL}s)", flush=True)
    print(f"[INFO] ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}", flush=True)
    print(f"[INFO] Lookback window: last {LOOKBACK_MINUTES}m", flush=True)
    print(f"[INFO] Master GeoJSON: {MASTER_GEOJSON}", flush=True)
    print(f"[INFO] Output dir: {OUTPUT_DIR}", flush=True)
    print(f"[INFO] GeoJSON HTTP port: {HTTP_PORT}", flush=True)

    master = load_master_geojson()
    print(f"[INFO] Loaded {len(master['features'])} zones from master GeoJSON", flush=True)
    start_geojson_server()

    # Generate initial empty state
    generate_tier_files(master, {})

    while True:
        demand_map = query_clickhouse()
        generate_tier_files(master, demand_map)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
