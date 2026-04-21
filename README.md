# Global Retail & Weather Impact Analytics Platform

## Architecture

```
capstone/
├── ingestion/                        # Task 1 – Python ingestion framework
│   ├── base.py                       #   Abstract base (config, logging, paths)
│   ├── api_extractor.py              #   REST API extractor (retry, pagination)
│   └── file_extractor.py             #   CSV/Parquet manual-upload ingester
│
├── processing/
│   ├── bronze/
│   │   ├── weather_bronze.py         # Task 2 – raw JSON → typed Parquet
│   │   └── sales_bronze.py           # Task 2 – raw Parquet → city-partitioned
│   ├── silver/
│   │   ├── weather_silver.py         # Clean + feature-engineer → Delta
│   │   └── sales_silver.py           # Aggregate daily sales → Delta
│   └── gold/
│       ├── sales_weather_gold.py     # Task 3 – join sales + weather (star schema)
│       ├── dim_date.py               # Dimension: date attributes
│       ├── dim_location.py           # Dimension: city surrogate keys
│       └── fact_sales_weather.py     # Fact table: sales + weather metrics
│
├── loader/
│   └── spark_loader.py               # Task 2 – generic Spark/Delta loader
│
├── scripts/
│   ├── generate_sample_data.py       # Historical sample data generator (2022-2024)
│   ├── generate_live_weather.py      # Live weather generator (mirrors Open-Meteo schema)
│   ├── generate_live_retail.py       # Live retail generator (mirrors UCI schema)
│   ├── cdc_manager.py                # CDC – watermark-based incremental ingestion
│   └── run_live_pipeline.py          # One-stop live pipeline runner
│
├── analysis/
│   ├── analysis.py                   # Task 4 – Spark SQL analytical queries
│   ├── cli_app.py                    # Task 5 – Python console analytics application
│   └── insights.md                  # Written insights
│
├── configs/
│   └── api_config.yaml              # Config-driven endpoints, cities, date ranges
│
├── data_lake/
│   ├── raw/                          # Immutable landing layer
│   │   ├── weather/open_meteo/city=<X>/   # Historical JSON files
│   │   ├── retail_sales/uci_online_retail/ # Historical Parquet partitions
│   │   └── live/cdc/                      # CDC watermark + event log
│   ├── bronze/                       # Typed, partitioned Parquet tables
│   ├── silver/                       # Cleaned Delta tables
│   └── gold/                         # Star schema Delta tables
│
├── manual-uploads/
│   └── retail_sales/uci_online_retail/    # Drop CSVs here (never directly in raw/)
│
├── main.py                           # Unified entry point (historical + live)
└── README.md
```

## Data Lake Structure (Medallion Architecture)

| Layer  | Format | Purpose                          | Partition      |
|--------|--------|----------------------------------|----------------|
| raw    | JSON / Parquet / CSV | Immutable landing — exact source bytes | city= or year=/month= |
| bronze | Parquet (Spark) | Typed, cleaned, city-labelled    | city=          |
| silver | Delta   | Aggregated, feature-engineered   | city=          |
| gold   | Delta   | Star schema (fact + dims)        | city= / location_id= |

## Cities & Date Ranges

| City       | Lat     | Lon     | Country |
|------------|---------|---------|---------|
| London     | 51.5074 | -0.1278 | GB      |
| Manchester | 53.4808 | -2.2426 | GB      |
| Birmingham | 52.4862 | -1.8904 | GB      |
| Leeds      | 53.8008 | -1.5491 | GB      |
| Glasgow    | 55.8642 | -4.2518 | GB      |

- **Historical data**: 2022-01-01 → 2024-12-31 (Open-Meteo archive + UCI Online Retail)
- **Live data**: Today onwards — generated daily, 500 retail records + weather per day per city

---

## Setup

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Copy `.env.example` → `.env` and set API keys (weather uses Open-Meteo which is free & keyless).

---

## Running the Historical Pipeline (Task 1–5)

```bash
# Step 1: Ingest weather (Open-Meteo, no key needed)
python main.py --entity weather

# Step 2: Ingest retail sales (place OnlineRetail.csv in manual-uploads first)
python main.py --entity retail_sales

# Step 3: Bronze transformations
python main.py --entity weather_bronze
python main.py --entity sales_bronze

# Step 4: Silver transformations
python main.py --entity weather_silver
python main.py --entity sales_silver

# Step 5: Gold / Star schema
python main.py --entity gold
python main.py --entity dim_date
python main.py --entity dim_location
python main.py --entity fact

# OR run everything at once:
python main.py --all

# Step 6: Analytics CLI app
python analysis/cli_app.py
```

---

## Running the Live Pipeline (CDC + Incremental Ingestion)

### How the live pipeline works

```
┌─────────────────────────────────────────────────────────────────────┐
│                       LIVE DATA FLOW                                │
│                                                                     │
│  generate_live_weather.py                                           │
│    → data_lake/raw/weather/open_meteo/city=<X>/                     │
│        weather_<city>_<date>_<date>_<ts>.json   (same schema as     │
│                                                  historical)        │
│                                                                     │
│  generate_live_retail.py                                            │
│    → manual-uploads/retail_sales/uci_online_retail/                 │
│        OnlineRetail_live_<date>_<ts>.csv         (same columns as   │
│                                                  UCI dataset)       │
│                                                                     │
│  cdc_manager.py                                                     │
│    → reads watermark.json  (last processed date per entity)         │
│    → scans raw layer for files NEWER than watermark                 │
│    → triggers WeatherBronzeTransformer / FileExtractor + SalesBronze│
│    → updates watermark + writes to cdc_events.jsonl                 │
│                                                                     │
│  Watermark file:  data_lake/raw/live/cdc/watermark.json             │
│  CDC event log:   data_lake/raw/live/cdc/cdc_events.jsonl           │
└─────────────────────────────────────────────────────────────────────┘
```

### One-time setup (backfill last 5 days)

```bash
python main.py --live backfill
```

This generates weather + retail data for the **5 days before today**, then runs CDC to process both into bronze. Run this **once** when setting up the live pipeline.

### Daily scheduled run (cron)

```bash
# Add to crontab:  0 6 * * * cd /path/to/capstone && python main.py --live daily
python main.py --live daily
```

Generates tomorrow's data (500 retail records + weather for all 5 cities), then runs CDC.

### Manual controls

```bash
# Generate files only (no CDC)
python main.py --live generate

# Run CDC only (files already generated)
python main.py --live cdc

# Dry-run CDC (see what would run, no changes)
python main.py --live cdc --dry-run

# Check watermark status and pending file counts
python main.py --live status

# Generate + CDC for a specific date
python main.py --live daily --date 2026-04-25
```

### CDC Watermark

The watermark file (`data_lake/raw/live/cdc/watermark.json`) tracks the last fully processed date per entity:

```json
{
  "weather":      "2026-04-19",
  "retail_sales": "2026-04-19"
}
```

On every CDC run, only files **newer than the watermark** are processed. After successful processing, the watermark is advanced to the latest date found. This ensures idempotency — re-running CDC will not reprocess already-handled files.

### CDC Event Log

Every CDC run appends structured events to `data_lake/raw/live/cdc/cdc_events.jsonl`:

```json
{"entity": "weather", "status": "processing", "watermark_before": "2026-04-18",
 "new_files": ["..."], "new_file_count": 5, "max_date": "2026-04-19", "logged_at": "..."}
{"entity": "weather", "status": "complete",   "watermark_after": "2026-04-19",
 "files_processed": 5, "logged_at": "..."}
```

---

## Generated Data Schema

### Weather (mirrors Open-Meteo archive format)

| Field                | Type   | Example   |
|----------------------|--------|-----------|
| latitude             | float  | 51.5074   |
| longitude            | float  | -0.1278   |
| timezone             | string | Europe/London |
| daily.time           | [date] | ["2026-04-19"] |
| daily.temperature_2m_max | [float] | [14.2] |
| daily.temperature_2m_min | [float] | [8.5]  |
| daily.temperature_2m_mean| [float] | [11.3] |
| daily.precipitation_sum  | [float] | [0.0]  |
| daily.rain_sum           | [float] | [0.0]  |
| daily.snowfall_sum       | [float] | [0.0]  |
| daily.windspeed_10m_max  | [float] | [9.1]  |
| daily.weathercode        | [int]   | [2]    |
| _meta.source         | string | open_meteo_live_SYNTHETIC |

### Retail (mirrors UCI Online Retail CSV)

| Column      | Type    | Example                              |
|-------------|---------|--------------------------------------|
| InvoiceNo   | string  | 600123 (prefix C = cancellation)     |
| StockCode   | string  | 85123A                               |
| Description | string  | WHITE HANGING HEART T-LIGHT HOLDER   |
| Quantity    | int     | 6 (negative for cancellations)       |
| InvoiceDate | string  | 4/19/2026 14:32 (M/D/YYYY HH:MM)    |
| UnitPrice   | float   | 2.55                                 |
| CustomerID  | float   | 17850.0                              |
| Country     | string  | United Kingdom                       |

City is derived in `SalesBronzeTransformer` via `CustomerID % 5` (same as historical).

---

## Star Schema (Gold Layer)

```
                    ┌─────────────┐
                    │  dim_date   │
                    │─────────────│
                    │ date_id (PK)│
                    │ year        │
                    │ month       │
                    │ day         │
                    │ day_of_week │
                    │ week_of_year│
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────┴───────────────┐    ┌──────────────┐
│ dim_location │    │  fact_sales_weather  │    │              │
│──────────────│    │──────────────────────│    │              │
│location_id PK├───►│ date_id (FK)         │    │              │
│ city         │    │ location_id (FK)     │◄───┤              │
└──────────────┘    │ total_sales          │    └──────────────┘
                    │ avg_temp             │
                    │ precipitation        │
                    │ is_rainy             │
                    │ rolling_7d_sales     │
                    └──────────────────────┘
```

---

## Assumptions

- Retail data is the UCI Online Retail dataset (UK transactions, 2010–2011), date-shifted +12 years to align with weather data (2022–2024).
- Live data generation is synthetic but schema-identical to historical sources — both feed the same bronze pipeline unchanged.
- City assignment uses `CustomerID % 5` (same in both historical bronze and live generation).
- Historical weather uses Open-Meteo archive API (free, no key). Live weather is synthetic but uses identical JSON structure.
- All timestamps are UTC. Raw layer is append-only / immutable.
- CDC watermarks are file-level (by date encoded in filename), not row-level.
- 500 retail records/day (~3 % are cancellations with negative quantity, matching UCI patterns).
