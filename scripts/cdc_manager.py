"""
scripts/cdc_manager.py

CDC (Change Data Capture) – Incremental Ingestion Simulation
=============================================================

How it works
------------
1. Maintains a lightweight "watermark" log (JSON file) that records the
   last successfully processed date for each entity (weather, retail_sales).

2. On each run, scans the raw layer for NEW files that are AFTER the watermark.

3. Emits a CDC event log (JSON-L) capturing what was found and what the
   new watermark is.

4. Plugs into the LIVE bronze → silver → gold pipeline:
      weather raw  → WeatherBronzeLiveTransformer  → WeatherSilverLiveTransformer
      retail  raw  → FileExtractor (live)           → SalesBronzeLiveTransformer
                   → SalesSilverLiveTransformer

   After silver is ready, SalesWeatherGoldLive rebuilds the live gold table
   so the CLI analytics app immediately sees the new data.

Watermark file:  data_lake/raw/live/cdc/watermark.json
CDC log:         data_lake/raw/live/cdc/cdc_events.jsonl

Usage
-----
    python scripts/cdc_manager.py --entity all
    python scripts/cdc_manager.py --entity weather
    python scripts/cdc_manager.py --entity retail_sales
    python scripts/cdc_manager.py --entity all --dry-run
    python scripts/cdc_manager.py --status
    python scripts/cdc_manager.py --reset --entity weather
"""

import argparse
import json
import logging
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Paths ────────────────────────────────────────────────────────────────────
CDC_DIR        = Path("data_lake/raw/live/cdc")
WATERMARK_FILE = CDC_DIR / "watermark.json"
CDC_LOG_FILE   = CDC_DIR / "cdc_events.jsonl"

WEATHER_RAW_DIR = Path("data_lake/raw/weather/open_meteo")
RETAIL_RAW_DIR  = Path("manual-uploads/retail_sales/uci_online_retail")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [CDC] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cdc_manager")


# ── Watermark ────────────────────────────────────────────────────────────────

def load_watermark() -> dict:
    if WATERMARK_FILE.exists():
        with open(WATERMARK_FILE) as f:
            return json.load(f)
    return {}


def save_watermark(wm: dict):
    CDC_DIR.mkdir(parents=True, exist_ok=True)
    with open(WATERMARK_FILE, "w") as f:
        json.dump(wm, f, indent=2)
    logger.info(f"Watermark saved → {WATERMARK_FILE}")


def get_entity_watermark(entity: str) -> Optional[date]:
    val = load_watermark().get(entity)
    return date.fromisoformat(val) if val else None


def set_entity_watermark(entity: str, new_date: date, dry_run: bool = False):
    if dry_run:
        logger.info(f"[DRY-RUN] Would update watermark[{entity}] → {new_date}")
        return
    wm = load_watermark()
    wm[entity] = str(new_date)
    save_watermark(wm)


# ── CDC Event log ─────────────────────────────────────────────────────────────

def log_cdc_event(event: dict):
    CDC_DIR.mkdir(parents=True, exist_ok=True)
    event["logged_at"] = datetime.utcnow().isoformat()
    with open(CDC_LOG_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")


# ── File discovery ────────────────────────────────────────────────────────────

def _date_from_weather_fname(fname: str) -> Optional[date]:
    m = re.search(r'weather_\w+_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_', fname)
    if m:
        return date.fromisoformat(m.group(2))
    hits = re.findall(r'\d{4}-\d{2}-\d{2}', fname)
    return date.fromisoformat(hits[-1]) if hits else None


def _date_from_retail_fname(fname: str) -> Optional[date]:
    m = re.search(r'live_(\d{4}-\d{2}-\d{2})_', fname)
    return date.fromisoformat(m.group(1)) if m else None


def discover_new_weather_files(after: Optional[date]) -> list:
    results = []
    for city_dir in sorted(WEATHER_RAW_DIR.glob("city=*")):
        for fpath in sorted(city_dir.glob("*.json")):
            file_date = _date_from_weather_fname(fpath.name)
            if file_date is None:
                continue
            # Only live files: 2025-01-01 onwards
            if file_date < date(2025, 1, 1):
                continue
            if after is None or file_date > after:
                results.append((file_date, fpath))
    return sorted(results, key=lambda x: x[0])


def discover_new_retail_files(after: Optional[date]) -> list:
    results = []
    for fpath in sorted(RETAIL_RAW_DIR.glob("OnlineRetail_live_*.csv")):
        file_date = _date_from_retail_fname(fpath.name)
        if file_date is None:
            continue
        if after is None or file_date > after:
            results.append((file_date, fpath))
    return sorted(results, key=lambda x: x[0])


# ── Bronze / Silver / Gold triggers ──────────────────────────────────────────

def _run_live_pipeline(dry_run: bool):
    """
    After new files are staged, rebuild the full LIVE medallion stack:
      bronze_live → silver_live → gold_live
    """
    if dry_run:
        logger.info("[DRY-RUN] Would run: WeatherBronzeLive → WeatherSilverLive → "
                    "SalesBronzeLive → SalesSilverLive → SalesWeatherGoldLive")
        return

    steps = [
        ("WeatherBronzeLiveTransformer",
         "processing.bronze.weather_bronze_live", "WeatherBronzeLiveTransformer"),
        ("WeatherSilverLiveTransformer",
         "processing.silver.weather_silver_live", "WeatherSilverLiveTransformer"),
        ("SalesBronzeLiveTransformer",
         "processing.bronze.sales_bronze_live",   "SalesBronzeLiveTransformer"),
        ("SalesSilverLiveTransformer",
         "processing.silver.sales_silver_live",   "SalesSilverLiveTransformer"),
        ("SalesWeatherGoldLive",
         "processing.gold.sales_weather_gold_live", "SalesWeatherGoldLive"),
    ]

    for label, module_path, class_name in steps:
        logger.info(f"  Running {label}...")
        try:
            import importlib
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            cls().extract()
            logger.info(f"  ✔ {label} complete")
        except ImportError:
            logger.warning(f"  {label} not importable (PySpark not configured). "
                           f"Run manually: python main.py --live-stage {label}")
        except Exception as e:
            logger.error(f"  {label} failed: {e}", exc_info=True)


def _ingest_retail_files(new_files: list, dry_run: bool):
    """
    Use FileExtractor to copy live retail CSVs from manual-uploads →
    data_lake/raw/retail_sales/uci_online_retail_live/
    """
    if dry_run:
        logger.info(f"[DRY-RUN] Would ingest {len(new_files)} retail file(s) via FileExtractor")
        return

    try:
        from ingestion.file_extractor import FileExtractor

        class _LiveFileExtractor(FileExtractor):
            """Overrides source dir to point at live CSVs."""
            MANUAL_UPLOADS_ROOT = Path("manual-uploads")

        extractor = _LiveFileExtractor.__new__(_LiveFileExtractor)
        FileExtractor.__init__(extractor, config_path="configs/api_config.yaml")

        # Override the raw output to a live-specific sub-path
        original_build = extractor._build_output_path

        def _live_output_path(entity, source_name, partition=None):
            return original_build(entity, source_name + "_live", partition)

        extractor._build_output_path = _live_output_path

        for _, fpath in new_files:
            logger.info(f"  Ingesting {fpath.name}...")
            extractor.extract(
                entity="retail_sales",
                source_name="uci_online_retail",
                file_name=fpath.name,
                output_format="parquet",
            )
    except Exception as e:
        logger.error(f"  FileExtractor failed: {e}", exc_info=True)


# ── Core CDC runners ──────────────────────────────────────────────────────────

def run_cdc_weather(dry_run: bool = False):
    logger.info("=" * 60)
    logger.info("CDC: Weather – scanning for new live files")
    logger.info("=" * 60)

    watermark = get_entity_watermark("weather")
    logger.info(f"Current watermark: {watermark or 'None (first run)'}")

    new_files = discover_new_weather_files(after=watermark)

    if not new_files:
        logger.info("No new weather files found.")
        log_cdc_event({"entity": "weather", "status": "no_new_data",
                       "watermark_before": str(watermark)})
        return False

    max_date   = max(d for d, _ in new_files)
    file_paths = [p for _, p in new_files]

    logger.info(f"Found {len(new_files)} new file(s). Dates: "
                f"{min(d for d,_ in new_files)} → {max_date}")
    for dt, fp in new_files:
        logger.info(f"  [{dt}]  {fp.name}")

    log_cdc_event({"entity": "weather", "status": "processing",
                   "watermark_before": str(watermark),
                   "new_file_count": len(new_files),
                   "max_date": str(max_date), "dry_run": dry_run})

    set_entity_watermark("weather", max_date, dry_run)

    log_cdc_event({"entity": "weather", "status": "staged",
                   "watermark_after": str(max_date), "dry_run": dry_run})
    logger.info(f"CDC weather staged. New watermark: {max_date}")
    return True


def run_cdc_retail(dry_run: bool = False):
    logger.info("=" * 60)
    logger.info("CDC: Retail Sales – scanning for new live files")
    logger.info("=" * 60)

    watermark = get_entity_watermark("retail_sales")
    logger.info(f"Current watermark: {watermark or 'None (first run)'}")

    new_files = discover_new_retail_files(after=watermark)

    if not new_files:
        logger.info("No new retail files found.")
        log_cdc_event({"entity": "retail_sales", "status": "no_new_data",
                       "watermark_before": str(watermark)})
        return False

    max_date = max(d for d, _ in new_files)

    logger.info(f"Found {len(new_files)} new file(s). Dates: "
                f"{min(d for d,_ in new_files)} → {max_date}")
    for dt, fp in new_files:
        logger.info(f"  [{dt}]  {fp.name}")

    log_cdc_event({"entity": "retail_sales", "status": "processing",
                   "watermark_before": str(watermark),
                   "new_file_count": len(new_files),
                   "max_date": str(max_date), "dry_run": dry_run})

    _ingest_retail_files(new_files, dry_run)

    set_entity_watermark("retail_sales", max_date, dry_run)

    log_cdc_event({"entity": "retail_sales", "status": "staged",
                   "watermark_after": str(max_date), "dry_run": dry_run})
    logger.info(f"CDC retail staged. New watermark: {max_date}")
    return True


def run_cdc_all(dry_run: bool = False):
    """Detect new files for both entities, then rebuild the full live stack."""
    weather_new = run_cdc_weather(dry_run)
    retail_new  = run_cdc_retail(dry_run)

    if weather_new or retail_new:
        logger.info("New data detected – rebuilding LIVE medallion stack...")
        _run_live_pipeline(dry_run)
    else:
        logger.info("Nothing new. Live stack is up to date.")


def show_watermark_status():
    wm = load_watermark()
    print("\n── CDC Watermark Status ──────────────────────────────────")
    if not wm:
        print("  No watermark found. Run CDC to initialise.")
    else:
        for entity, wm_date in wm.items():
            print(f"  {entity:20s} → last processed: {wm_date}")
    print()
    weather_wm = get_entity_watermark("weather")
    retail_wm  = get_entity_watermark("retail_sales")
    pending_w  = discover_new_weather_files(after=weather_wm)
    pending_r  = discover_new_retail_files(after=retail_wm)
    print(f"  Pending weather files:  {len(pending_w)}")
    print(f"  Pending retail files:   {len(pending_r)}")
    print("──────────────────────────────────────────────────────────\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="CDC Manager – Incremental ingestion for live data"
    )
    parser.add_argument("--entity",  choices=["weather", "retail_sales", "all"], default="all")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--status",  action="store_true")
    parser.add_argument("--reset",   action="store_true")
    args = parser.parse_args()

    if args.status:
        show_watermark_status()
        return

    if args.reset:
        if args.entity == "all":
            print("Specify --entity weather or --entity retail_sales to reset.")
            return
        wm = load_watermark()
        wm.pop(args.entity, None)
        save_watermark(wm)
        logger.info(f"Watermark for '{args.entity}' reset.")
        return

    if args.dry_run:
        logger.info("*** DRY RUN MODE ***")

    if args.entity == "all":
        run_cdc_all(dry_run=args.dry_run)
    elif args.entity == "weather":
        run_cdc_weather(dry_run=args.dry_run)
    elif args.entity == "retail_sales":
        run_cdc_retail(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
