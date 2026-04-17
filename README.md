# Global Retail & Weather Impact Analytics Platform

## Architecture

```
data_lake/
├── raw/                          # Task 1 output — exact API/file bytes
│   ├── weather/
│   │   └── open_meteo/
│   │       └── city=London/
│   │           └── weather_london_2022-01-01_2024-12-31_<ts>.json
│   └── retail_sales/
│       └── uci_online_retail/
│           └── year=2011/month=01/
│               └── Online_Retail_<ts>.parquet
├── bronze/                       # Task 2 — typed Delta tables
├── silver/                       # Task 2 — cleaned, conformed
└── gold/                         # Task 3/4 — star schema
```

**Manual uploads** go in `manual-uploads/<entity>/<source>/` — never directly in `data_lake/raw/`.

## Cities & Date Range

| City       | Lat     | Lon     |
|------------|---------|---------|
| London     | 51.5074 | -0.1278 |
| Manchester | 53.4808 | -2.2426 |
| Birmingham | 52.4862 | -1.8904 |
| Leeds      | 53.8008 | -1.5491 |
| Glasgow    | 55.8642 | -4.2518 |

Date range: **2022-01-01 → 2024-12-31** (3 years, daily granularity)

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and add your API keys (weather uses Open-Meteo which is free & keyless).

## Running Ingestion (Task 1)

```bash
# Weather (Open-Meteo, no key needed)
python main.py --entity weather

# Retail sales (place CSV in manual-uploads/retail_sales/uci_online_retail/)
python main.py --entity retail_sales --file "Online Retail.csv"

# Both
python main.py --all
```

## Assumptions

- Retail data is the UCI Online Retail dataset (UK transactions, 2010–2011).
- Weather data covers 2022–2024 via Open-Meteo archive (free, no key).
- 5 UK cities chosen to match the retail dataset's geography.
- All timestamps stored as UTC.
- Raw layer is immutable; transformations happen in bronze → silver → gold.
