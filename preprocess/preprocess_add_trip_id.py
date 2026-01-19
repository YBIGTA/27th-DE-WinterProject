from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from tqdm import tqdm


INPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "taxi_data_preprocessed"
OUTPUT_DIR = (
    Path(__file__).resolve().parent.parent / "data" / "taxi_data_preprocessed_with_trip_id"
)
MAX_SEQ = 1_000_000_000
def _append_trip_id(table: pa.Table, counters: dict[str, int]) -> pa.Table:
    if "tpep_pickup_datetime" not in table.column_names:
        raise ValueError("Missing tpep_pickup_datetime column.")

    date_prefix = pc.strftime(table["tpep_pickup_datetime"], format="%y%m%d")
    prefixes = date_prefix.to_pylist()
    trip_ids: list[int | None] = []
    for prefix in prefixes:
        if prefix is None:
            trip_ids.append(None)
            continue
        next_count = counters.get(prefix, 0) + 1
        if next_count > MAX_SEQ:
            raise ValueError(f"Trip ID sequence exceeded {MAX_SEQ} for date {prefix}")
        counters[prefix] = next_count
        trip_ids.append(int(prefix) * MAX_SEQ + next_count)
    return table.append_column("trip_id", pa.array(trip_ids, type=pa.int64()))


def process_file(src_path: Path, dst_path: Path) -> None:
    if src_path.stat().st_size == 0:
        return

    pf = pq.ParquetFile(src_path)
    if "trip_id" in pf.schema_arrow.names:
        raise ValueError(f"{src_path} already contains trip_id")

    table = pq.read_table(src_path)
    sort_indices = pc.sort_indices(table["tpep_pickup_datetime"])
    table = table.take(sort_indices)
    table = _append_trip_id(table, {})

    dst_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, dst_path, compression="zstd")


def _process_file(args: tuple[str, str]) -> None:
    src_path, dst_path = args
    process_file(Path(src_path), Path(dst_path))


def main() -> None:
    if not INPUT_DIR.exists():
        raise SystemExit(f"Input directory not found: {INPUT_DIR}")

    files = sorted(INPUT_DIR.rglob("*.parquet"))
    processed = 0
    file_pairs = [
        (str(src_path), str(OUTPUT_DIR / src_path.relative_to(INPUT_DIR)))
        for src_path in files
    ]

    max_workers = 4
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for _ in tqdm(
            executor.map(_process_file, file_pairs),
            total=len(file_pairs),
            desc="Adding trip_id",
            unit="file",
        ):
            processed += 1

    print("Trip ID summary")
    print("Processed files:", processed)
    print("Output dir:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
