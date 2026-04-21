# Changes in v2 — analysis/cli_app.py

## Problem (v1)
Selecting **[L] Live** in the CLI tried to read `data_lake/gold/sales_weather_live`
directly.  If the live medallion stack had never been built (or was stale) the
load would fail with "table not found".  The historical [H] path had the same
potential issue — the raw → bronze → silver → gold chain was never triggered
from inside the CLI.

## Fix (v2)

### 1. Pipeline runners wired into the CLI
Two new functions in `cli_app.py`:

| Function | What it runs |
|---|---|
| `run_historical_pipeline()` | Ingest → weather_bronze → sales_bronze → weather_silver → sales_silver → dim_date/location → gold → fact |
| `run_live_pipeline()` | backfill (5 days) → CDC → weather_bronze_live → weather_silver_live → sales_bronze_live → sales_silver_live → gold_live |

### 2. Menus updated
The dataset selector now has **three options**:

| Key | Action |
|---|---|
| **H** | Prompts "run pipeline?" → if yes, runs `run_historical_pipeline()` → loads `sales_weather` gold → analytics menu |
| **L** | Prompts "run pipeline?" → if yes, runs `run_live_pipeline()` → loads `sales_weather_live` gold → analytics menu |
| **B** | Prompts "run both?" → builds both → runs all individual + comparison charts |

### 3. New comparison charts (B mode)
| Chart file | Content |
|---|---|
| `combined_city_comparison.png` | Grouped bar: avg daily sales per city, Hist vs Live |
| `combined_rain_impact.png` | 2-panel: rain vs dry sales, Hist (left) Live (right) |
| `combined_dashboard.png` | 2×2 grid: city, temp category, rain, monthly trend |
| `live_vs_rolling.png` | Per-city line chart: actual sales + 7-day rolling avg |
| `live_temp_scatter.png` | Scatter: temp vs sales per city (live window) |

### 4. Non-interactive flags
```
python analysis/cli_app.py --mode H               # historical pipeline + all charts
python analysis/cli_app.py --mode L               # live pipeline + all charts
python analysis/cli_app.py --mode B               # both pipelines + comparison
python analysis/cli_app.py --skip-pipeline --mode H  # skip pipeline, use existing gold
```

### 5. Colour coding
Historical charts → **blue** (`#4C72B0`)
Live charts       → **orange** (`#DD8452`)
Rain bars         → **green** (`#55A868`)
