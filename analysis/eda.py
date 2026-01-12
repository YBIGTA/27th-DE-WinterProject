from pathlib import Path
import re
import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt

# --- raw ver
# base_dir = Path(__file__).resolve().parent.parent / "data" / "taxi_data"

# --- preprocessed ver
base_dir = Path(__file__).resolve().parent.parent / "data" / "taxi_data_preprocessed"

files = sorted(base_dir.rglob("*.parquet"))
out_dir = Path(__file__).resolve().parent / "outputs"
out_dir.mkdir(parents=True, exist_ok=True)

# --- monthly counts via Parquet metadata ---
rows = []
pattern = re.compile(r"(\d{4})-(\d{2})")
for fp in files:
    m = pattern.search(fp.name)
    if not m:
        continue
    year, month = map(int, m.groups())
    pf = pq.ParquetFile(fp)
    rows.append({
        "year": year,
        "month": month,
        "file": str(fp),
        "row_count": pf.metadata.num_rows
    })

if not rows:
    raise SystemExit(f"No parquet files matched in {base_dir}")

counts = pd.DataFrame(rows).sort_values(["year", "month"]).reset_index(drop=True)
counts["date"] = pd.to_datetime(counts["year"].astype(str) + "-" + counts["month"].astype(str) + "-01")
counts["mom_change"] = counts["row_count"].diff()
counts["mom_change_pct"] = counts["row_count"].pct_change() * 100

total_rows_2009_2025 = counts["row_count"].sum()

# Monthly stats summary
monthly_stats = counts[["date", "row_count", "mom_change", "mom_change_pct"]]

# Optional: yearly totals
yearly = counts.groupby("year", as_index=False)["row_count"].sum()

csv_paths = [
    out_dir / "monthly_counts.csv",
    out_dir / "monthly_stats.csv",
    out_dir / "yearly_totals.csv",
    out_dir / "schema_mismatches.csv",
]
for csv_path in csv_paths:
    if csv_path.exists():
        csv_path.unlink()

counts.to_csv(out_dir / "monthly_counts.csv", index=False)
monthly_stats.to_csv(out_dir / "monthly_stats.csv", index=False)
yearly.to_csv(out_dir / "yearly_totals.csv", index=False)

fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
axes[0].plot(monthly_stats["date"], monthly_stats["row_count"], color="#1f77b4")
axes[0].set_title("Monthly Row Count")
axes[0].set_ylabel("Rows")

axes[1].plot(monthly_stats["date"], monthly_stats["mom_change_pct"], color="#ff7f0e")
axes[1].axhline(0, color="#999999", linewidth=0.8)
axes[1].set_title("Month-over-Month Change (%)")
axes[1].set_ylabel("Percent")
axes[1].set_xlabel("Month")

fig.autofmt_xdate()
fig.tight_layout()
plot_path = out_dir / "monthly_stats.png"
fig.savefig(plot_path, dpi=150)
plt.close(fig)

# --- schema consistency check ---
baseline = None
baseline_file = None
schema_issues = []
column_presence = {}
column_files = {}
column_types = {}

for fp in files:
    pf = pq.ParquetFile(fp)
    schema = [(f.name, str(f.type)) for f in pf.schema_arrow]
    m = pattern.search(fp.name)
    file_label = f"{m.group(1)}-{m.group(2)}" if m else fp.stem
    if baseline is None:
        baseline = schema
        baseline_set = set(schema)
        baseline_file = str(fp)
    else:
        schema_set = set(schema)
        if schema_set != baseline_set:
            missing = baseline_set - schema_set
            extra = schema_set - baseline_set
            schema_issues.append({
                "file": str(fp),
                "missing_cols": sorted(missing),
                "extra_cols": sorted(extra),
            })
    for name, dtype in schema:
        column_presence[name] = column_presence.get(name, 0) + 1
        column_files.setdefault(name, []).append(file_label)
        column_types.setdefault(name, set()).add(dtype)

if schema_issues:
    schema_issues_df = pd.DataFrame(schema_issues)
    schema_issues_df.to_csv(out_dir / "schema_mismatches.csv", index=False)
else:
    schema_issues_df = None

baseline_df = pd.DataFrame(baseline, columns=["column", "type"])
baseline_df.to_csv(out_dir / "schema_baseline.csv", index=False)

schema_summary_rows = []
total_files = len(files)
for name in sorted(column_presence.keys()):
    types = sorted(column_types.get(name, set()))
    present_count = column_presence[name]
    schema_summary_rows.append({
        "column": name,
        "types_seen": "|".join(types),
        "files_with_column": present_count,
        "files_missing_column": total_files - present_count,
        "files_with_column_list": "|".join(sorted(column_files.get(name, []))),
    })
schema_summary_df = pd.DataFrame(schema_summary_rows)
schema_summary_df.to_csv(out_dir / "schema_column_summary.csv", index=False)

try:
    import matplotlib.pyplot as plt
except ImportError:
    plt = None

plot_path = None
if plt is not None:
    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
    axes[0].plot(monthly_stats["date"], monthly_stats["row_count"], color="#1f77b4")
    axes[0].set_title("Monthly Row Count")
    axes[0].set_ylabel("Rows")

    axes[1].plot(monthly_stats["date"], monthly_stats["mom_change_pct"], color="#ff7f0e")
    axes[1].axhline(0, color="#999999", linewidth=0.8)
    axes[1].set_title("Month-over-Month Change (%)")
    axes[1].set_ylabel("Percent")
    axes[1].set_xlabel("Month")

    fig.autofmt_xdate()
    fig.tight_layout()
    plot_path = out_dir / "monthly_stats.png"
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)

print("Summary")
print("Total rows (2009–2025):", total_rows_2009_2025)
print("Parquet files scanned:", len(files))
print("Date range:", counts["date"].min().date(), "to", counts["date"].max().date())
print("Schema mismatches:", 0 if schema_issues_df is None else len(schema_issues_df))
print("Wrote CSVs to:", out_dir)
print("Baseline schema file:", baseline_file)
print("Wrote schema baseline to:", out_dir / "schema_baseline.csv")
print("Wrote schema summary to:", out_dir / "schema_column_summary.csv")
print("Wrote plot to:", plot_path if plot_path is not None else "skipped (matplotlib not installed)")
