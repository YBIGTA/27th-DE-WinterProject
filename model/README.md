# Model Runtime Guide

## Core files
- Preprocessing notebook: `model/notebooks/01_data_preprocessing.ipynb`
- Training/export notebook: `model/notebooks/02_feature_engineering_and_train.ipynb`
- ONNX backtest notebook: `model/notebooks/03_onnx_backtest_2024.ipynb`
- Python dependencies: `model/requirements.txt`
- Model artifact output: `model/models/taxi_demand_model.onnx`
- Processed dataset output: `model/data/processed/aggregated_demand_3min.parquet`

## Scope
This folder builds a 3-minute step taxi-demand forecasting model, exports it to ONNX for serving, and validates ranking/spike quality on 2024 out-of-time data.

## Environment setup
Run from project root:

```bash
cd model
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip setuptools wheel
pip install -r requirements.txt
```

Optional Jupyter kernel registration:

```bash
python -m ipykernel install --user --name model-venv --display-name "Python (model-venv)"
```

## Dependencies
Installed via `model/requirements.txt`:
- `polars`
- `lightgbm`
- `scikit-learn`
- `onnxmltools`
- `skl2onnx`
- `jupyterlab`
- `matplotlib`
- `pyarrow`
- `pandas`
- `onnxruntime`

## Run order
Use the same kernel (`Python (model-venv)`) for all notebooks.

### 1) Preprocess raw trip data
```bash
cd model
source .venv/bin/activate
jupyter lab notebooks/01_data_preprocessing.ipynb
```

Expected output:
- `model/data/processed/aggregated_demand_3min.parquet`

### 2) Train and export ONNX
```bash
cd model
source .venv/bin/activate
jupyter lab notebooks/02_feature_engineering_and_train.ipynb
```

Main runtime parameters in notebook 02:
- `RECENT_MONTHS = 6`
- `TEST_MONTHS = 1`
- `FEATURE_MODE = "lite"` (or `"full"`)
- `HORIZON_STEPS = 5` (3 min x 5 = 15 min ahead)

Expected outputs:
- Training/test split logs
- MAE/RMSE + zone/time/bias diagnostics
- ONNX export success log
- `model/models/taxi_demand_model.onnx`
- ONNX parity check output (`Mean abs diff`, `Max abs diff`, `Pass rate`)

### 3) 2024 ONNX backtest
```bash
cd model
source .venv/bin/activate
jupyter lab notebooks/03_onnx_backtest_2024.ipynb
```

Main runtime parameters in notebook 03:
- `DATA_PATH = "../data/processed/aggregated_demand_3min.parquet"`
- `ONNX_PATH = "../models/taxi_demand_model.onnx"`
- `FEATURE_MODE = "lite"` (must match training-time schema)
- `HORIZON_STEPS = 5`
- `BACKTEST_START = 2024-10-01`
- `BACKTEST_END = 2025-01-01`

Expected outputs:
- Overall metrics (MAE/RMSE/Bias, naive comparison)
- Zone split metrics
- Time bucket metrics
- Top-K ranking metrics (`K = 3, 5, 10`)
- Spike detection metrics (zone-relative quantile threshold)
- PASS/FAIL summary

## Quick validation checklist
1. ONNX parity check in notebook 02 passes with near-zero diff.
2. Notebook 03 runs without schema mismatch error.
3. Top-K metrics outperform naive baseline for target serving K (for example K=5).
4. Artifact `model/models/taxi_demand_model.onnx` is regenerated after retraining.

## Troubleshooting
- `Train samples: 0`:
  - `RECENT_MONTHS` / `TEST_MONTHS` window is too tight for current data coverage.
  - Reduce `TEST_MONTHS` or increase `RECENT_MONTHS`.
- `name 'feature_names_for_onnx' is not defined`:
  - Use current notebook code path based on `features` (already updated).
- `onnxruntime not installed`:
  - `pip install onnxruntime`
- Kernel crash during large training:
  - Reduce date range, use `FEATURE_MODE="lite"`, or increase machine memory.
