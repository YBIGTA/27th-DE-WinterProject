from __future__ import annotations

from pathlib import Path
import argparse
import csv
import statistics
import shapefile
from pyproj import CRS, Transformer


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SHAPE_PATH = ROOT / "data" / "taxi_zones" / "taxi_zones.shp"
DEFAULT_PRJ_PATH = ROOT / "data" / "taxi_zones" / "taxi_zones.prj"
DEFAULT_OUTPUT_PATH = ROOT / "data" / "taxi_zone_median_coords.csv"

SKIP_LOCATION_IDS = {264, 265}


def _load_transformer(prj_path: Path) -> Transformer:
    if prj_path.exists():
        source_crs = CRS.from_wkt(prj_path.read_text())
    else:
        source_crs = CRS.from_epsg(2263)
    return Transformer.from_crs(source_crs, CRS.from_epsg(4326), always_xy=True)


def _find_location_field(field_names: list[str]) -> str:
    candidates = {"locationid", "location_id", "loc_id"}
    for name in field_names:
        if name.lower() in candidates:
            return name
    raise SystemExit(f"LocationID field not found. Available fields: {', '.join(field_names)}")


def _median_lon_lat(points: list[tuple[float, float]], transformer: Transformer) -> tuple[float, float]:
    lons: list[float] = []
    lats: list[float] = []
    for x, y in points:
        lon, lat = transformer.transform(x, y)
        lons.append(lon)
        lats.append(lat)
    return statistics.median(lons), statistics.median(lats)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute median lon/lat per taxi zone.")
    parser.add_argument("--shape", type=Path, default=DEFAULT_SHAPE_PATH)
    parser.add_argument("--prj", type=Path, default=DEFAULT_PRJ_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--include-unknown", action="store_true")
    args = parser.parse_args()

    if not args.shape.exists():
        raise SystemExit(f"Shapefile not found: {args.shape}")

    transformer = _load_transformer(args.prj)
    reader = shapefile.Reader(str(args.shape))

    field_names = [field[0] for field in reader.fields[1:]]
    location_field = _find_location_field(field_names)
    location_index = field_names.index(location_field)

    rows: list[tuple[int, float, float]] = []
    for record, shape in zip(reader.iterRecords(), reader.iterShapes()):
        raw_location_id = record[location_index]
        if raw_location_id in (None, ""):
            continue
        try:
            location_id = int(raw_location_id)
        except (TypeError, ValueError):
            continue

        if location_id in SKIP_LOCATION_IDS and not args.include_unknown:
            continue
        if not shape.points:
            continue

        median_lon, median_lat = _median_lon_lat(shape.points, transformer)
        rows.append((location_id, median_lon, median_lat))

    rows.sort(key=lambda row: row[0])

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["LocationID", "longitude", "latitude"])
        writer.writerows(rows)

    print(f"Wrote {len(rows)} zones to {args.output}")


if __name__ == "__main__":
    main()
