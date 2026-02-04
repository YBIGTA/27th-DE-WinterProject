from pathlib import Path
import re

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from tqdm import tqdm


INPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "taxi_data"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "taxi_data_preprocessed2"

YEAR_MIN = 2011
YEAR_MAX = 2025
YEAR_PATTERN = re.compile(r"(\d{4})-(\d{2})")

KEEP_COLUMNS = [
    "DOLocationID",
    "PULocationID",
    "RatecodeID",
    "VendorID",
    "congestion_surcharge",
    "extra",
    "fare_amount",
    "improvement_surcharge",
    "mta_tax",
    "passenger_count",
    "payment_type",
    "store_and_fwd_flag",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "tpep_dropoff_datetime",
    "tpep_pickup_datetime",
    "trip_distance",
    "Airport_fee",
]

TARGET_TYPES = {
    "DOLocationID": pa.int64(),
    "PULocationID": pa.int64(),
    "RatecodeID": pa.int64(),
    "VendorID": pa.int64(),
    "congestion_surcharge": pa.float64(),
    "extra": pa.float64(),
    "fare_amount": pa.float64(),
    "improvement_surcharge": pa.float64(),
    "mta_tax": pa.float64(),
    "passenger_count": pa.int64(),
    "payment_type": pa.int64(),
    "store_and_fwd_flag": pa.string(),
    "tip_amount": pa.float64(),
    "tolls_amount": pa.float64(),
    "total_amount": pa.float64(),
    "tpep_dropoff_datetime": pa.timestamp("us"),
    "tpep_pickup_datetime": pa.timestamp("us"),
    "trip_distance": pa.float64(),
    "Airport_fee": pa.float64(),
}
TARGET_SCHEMA = pa.schema([pa.field(name, TARGET_TYPES[name]) for name in KEEP_COLUMNS])


def _year_in_range(filename: str) -> bool:
    match = YEAR_PATTERN.search(filename)
    if not match:
        return False
    year = int(match.group(1))
    return YEAR_MIN <= year <= YEAR_MAX


def _label_from_filename(filename: str) -> str:
    match = YEAR_PATTERN.search(filename)
    if not match:
        return Path(filename).stem
    return f"{match.group(1)}-{match.group(2)}"

def _expected_year_month(filename: str) -> tuple[int, int] | None:
    match = YEAR_PATTERN.search(filename)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def preprocess_file(
    src_path: Path, dst_path: Path, expected_year_month: tuple[int, int] | None
) -> None:
    if src_path.stat().st_size == 0:
        return

    pf = pq.ParquetFile(src_path)
    available = set(pf.schema_arrow.names)

    read_cols = []
    for name in KEEP_COLUMNS:
        if name in available:
            read_cols.append(name)
        elif name == "Airport_fee" and "airport_fee" in available:
            read_cols.append("airport_fee")

    table = pq.read_table(src_path, columns=read_cols)

    if "airport_fee" in table.column_names:
        new_names = ["Airport_fee" if col == "airport_fee" else col for col in table.column_names]
        table = table.rename_columns(new_names)

    for name in KEEP_COLUMNS:
        if name not in table.column_names:
            table = table.append_column(name, pa.nulls(table.num_rows, type=TARGET_TYPES[name]))

    table = table.select(KEEP_COLUMNS)
    table = table.cast(TARGET_SCHEMA, safe=False)

    mask = None
    if expected_year_month is not None:
        expected_year, expected_month = expected_year_month
        pickup_year = pc.year(table["tpep_pickup_datetime"])
        pickup_month = pc.month(table["tpep_pickup_datetime"])
        in_expected_month = pc.and_(
            pc.equal(pickup_year, expected_year),
            pc.equal(pickup_month, expected_month),
        )
        mask = in_expected_month

    airport_ids = pa.array([264, 265], type=pa.int64())
    pu_is_airport = pc.is_in(table["PULocationID"], value_set=airport_ids)
    do_is_airport = pc.is_in(table["DOLocationID"], value_set=airport_ids)
    not_airport = pc.and_(pc.invert(pu_is_airport), pc.invert(do_is_airport))
    mask = not_airport if mask is None else pc.and_(mask, not_airport)

    valid_time = pc.greater_equal(
        table["tpep_dropoff_datetime"], table["tpep_pickup_datetime"]
    )
    mask = valid_time if mask is None else pc.and_(mask, valid_time)

    table = table.filter(mask)

    dst_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, dst_path, compression="zstd")


def main() -> None:
    if not INPUT_DIR.exists():
        raise SystemExit(f"Input directory not found: {INPUT_DIR}")

    files = sorted(INPUT_DIR.rglob("*.parquet"))
    processed = 0
    skipped_empty = 0
    skipped_out_of_range = 0

    for src_path in tqdm(files, desc="Preprocessing", unit="file"):
        if not _year_in_range(src_path.name):
            skipped_out_of_range += 1
            continue
        if src_path.stat().st_size == 0:
            skipped_empty += 1
            continue

        rel_path = src_path.relative_to(INPUT_DIR)
        dst_path = OUTPUT_DIR / rel_path
        expected_year_month = _expected_year_month(src_path.name)
        preprocess_file(src_path, dst_path, expected_year_month)
        processed += 1

    print("Preprocess summary")
    print("Processed files:", processed)
    print("Skipped (empty):", skipped_empty)
    print("Skipped (out of range):", skipped_out_of_range)
    print("Output dir:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
