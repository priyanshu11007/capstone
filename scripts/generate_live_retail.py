"""
scripts/generate_live_retail.py

Generates LIVE synthetic Online Retail transactions that exactly mirror
the UCI Online Retail CSV columns used in your historical data.

Historical columns (from your SalesBronzeTransformer & FileExtractor):
    InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice,
    CustomerID, Country

What it does
------------
1. BACKFILL  – generates 500 records × 5 days (2500 total) as CSV files,
               one per day, placed in:
               manual-uploads/retail_sales/uci_online_retail/

2. DAILY RUN – generates 500 records for tomorrow (or --date),
               placed in the same folder so your FileExtractor picks
               them up automatically.

City assignment
---------------
CustomerID % 5 drives city assignment, mirroring SalesBronzeTransformer:
    0 → London, 1 → Manchester, 2 → Birmingham, 3 → Leeds, 4 → Glasgow

Usage
-----
    # Backfill last 5 days (run once)
    python scripts/generate_live_retail.py --mode backfill

    # Generate tomorrow's 500 records (run daily)
    python scripts/generate_live_retail.py --mode daily

    # Generate for a specific date (testing)
    python scripts/generate_live_retail.py --mode daily --date 2026-04-21
"""

import argparse
import csv
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# ── ensure project root on path ────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Constants ───────────────────────────────────────────────────────────────
RECORDS_PER_DAY = 500

CITIES = ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"]

# UCI Online Retail – product catalogue (StockCode, Description, UnitPrice)
PRODUCTS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER",   2.55),
    ("71053",  "WHITE METAL LANTERN",                   3.39),
    ("84406B", "CREAM CUPID HEARTS COAT HANGER",        2.75),
    ("84029G", "KNITTED UNION FLAG HOT WATER BOTTLE",   3.39),
    ("84029E", "RED WOOLLY HOTTIE WHITE HEART",          3.39),
    ("22752",  "SET 7 BABUSHKA NESTING BOXES",           7.65),
    ("21730",  "GLASS STAR FROSTED T-LIGHT HOLDER",     4.25),
    ("22633",  "HAND WARMER UNION JACK",                 1.85),
    ("22632",  "HAND WARMER RED POLKA DOT",              1.85),
    ("20727",  "LUNCH BAG RED RETROSPOT",                1.65),
    ("23203",  "JUMBO BAG VINTAGE LEAF",                 1.65),
    ("23205",  "JUMBO BAG RED RETROSPOT",                1.65),
    ("22720",  "SET OF 3 CAKE TINS PANTRY DESIGN",      6.95),
    ("22111",  "SCOTTIE DOG HOT WATER BOTTLE",           4.65),
    ("21212",  "PACK OF 72 RETROSPOT CAKE CASES",        0.55),
    ("22492",  "MINI PAINT SET VINTAGE",                 0.65),
    ("22382",  "LUNCH BAG SUKI DESIGN",                  1.65),
    ("20719",  "WOODLAND PARTY BAG + STICKER SET",       0.85),
    ("90019A", "MULTI COLOUR SILVER T-LIGHT HOLDER",    0.85),
    ("22423",  "REGENCY CAKESTAND 3 TIER",              12.75),
    ("47566",  "PARTY BUNTING",                          4.95),
    ("22720",  "SET OF 3 CAKE TINS PANTRY DESIGN",      6.95),
    ("85099B", "JUMBO BAG RED RETROSPOT",                1.65),
    ("22197",  "POPCORN HOLDER",                         0.85),
    ("84997C", "CHILDRENS CUTLERY DOLLY GIRL",           3.75),
    ("22699",  "ROSES REGENCY TEACUP AND SAUCER",        2.95),
    ("22698",  "PANSIES REGENCY TEACUP AND SAUCER",      2.95),
    ("22697",  "DAISIES REGENCY TEACUP AND SAUCER",      2.95),
    ("22355",  "CHARLOTTE BAG SUKI DESIGN",              1.65),
    ("22356",  "CHARLOTTE BAG DOLLY DESIGN",             1.65),
    ("21977",  "PACK OF 60 PINK PAISLEY CAKE CASES",     0.55),
    ("21212",  "PACK OF 72 RETROSPOT CAKE CASES",        0.55),
    ("22551",  "PLASTERS IN TIN CIRCUS PARADE",          1.65),
    ("22557",  "PLASTERS IN TIN WOODLAND ANIMALS",       1.65),
    ("22554",  "PLASTERS IN TIN STRONGMAN",              1.65),
    ("22556",  "PLASTERS IN TIN SPACEBOY",               1.65),
    ("48185",  "DOORMAT NEW ENGLAND",                    7.08),
    ("22502",  "PICNIC BASKET WICKER 60 PIECES",        12.75),
    ("22659",  "LUNCH BOX I LOVE LONDON",                1.65),
    ("22660",  "LUNCH BOX CARS BLUE",                    1.65),
]

# Countries mapped to cities
CITY_COUNTRY = {
    "London":     "United Kingdom",
    "Manchester": "United Kingdom",
    "Birmingham": "United Kingdom",
    "Leeds":      "United Kingdom",
    "Glasgow":    "United Kingdom",
}

# Customer ID pool: 12346–18287 (realistic UCI range)
CUSTOMER_ID_POOL = list(range(12346, 18288))

# InvoiceNo format: C-prefixed are cancellations (keep small %)
_INVOICE_COUNTER = [600000]  # mutable counter


def _next_invoice() -> str:
    _INVOICE_COUNTER[0] += 1
    # ~3 % cancellations (negative qty), prefix C
    if random.random() < 0.03:
        return f"C{_INVOICE_COUNTER[0]}"
    return str(_INVOICE_COUNTER[0])


def _invoice_time(dt: date) -> str:
    """Random HH:MM within business hours (8:00–20:00), UCI datetime format."""
    hour   = random.randint(8, 19)
    minute = random.randint(0, 59)
    # UCI uses format: '1/12/2010 08:26' (M/D/YYYY HH:MM)
    return f"{dt.month}/{dt.day}/{dt.year} {hour:02d}:{minute:02d}"


def generate_day_retail(dt: date, n: int = RECORDS_PER_DAY) -> list[dict]:
    """
    Generate n retail transaction rows for date dt.
    Returns list of dicts matching UCI Online Retail columns.
    """
    rows = []
    # Pre-generate a realistic mix of invoices (one invoice can have many lines)
    n_invoices = max(1, n // random.randint(3, 6))
    invoices   = [_next_invoice() for _ in range(n_invoices)]

    for i in range(n):
        invoice_no = random.choice(invoices)
        is_cancel  = invoice_no.startswith("C")

        stock_code, description, unit_price = random.choice(PRODUCTS)

        # Seasonal price nudge ±10 %
        unit_price = round(unit_price * random.uniform(0.90, 1.10), 2)

        quantity = random.randint(1, 24)
        if is_cancel:
            quantity = -abs(quantity)

        customer_id = random.choice(CUSTOMER_ID_POOL)
        city        = CITIES[customer_id % 5]
        country     = CITY_COUNTRY[city]

        rows.append({
            # Explicit str() on InvoiceNo and StockCode prevents pandas from
            # inferring them as INT64 on CSV re-read → avoids Parquet type mismatch.
            "InvoiceNo":   str(invoice_no),
            "StockCode":   str(stock_code),
            "Description": str(description),
            "Quantity":    int(quantity),
            "InvoiceDate": _invoice_time(dt),
            "UnitPrice":   float(unit_price),
            "CustomerID":  float(customer_id),   # UCI stores as float; bronze casts to int
            "Country":     str(country),
        })

    return rows


# ── CSV writer ───────────────────────────────────────────────────────────────
CSV_COLUMNS = ["InvoiceNo", "StockCode", "Description", "Quantity",
               "InvoiceDate", "UnitPrice", "CustomerID", "Country"]

MANUAL_UPLOAD_DIR = Path("manual-uploads/retail_sales/uci_online_retail")


def write_day_csv(dt: date, rows: list[dict]) -> Path:
    """
    Write one CSV file for the given date.
    Named: OnlineRetail_live_YYYY-MM-DD_<timestamp>.csv
    Placed in: manual-uploads/retail_sales/uci_online_retail/
    """
    MANUAL_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    ts    = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    fname = f"OnlineRetail_live_{dt.isoformat()}_{ts}.csv"
    path  = MANUAL_UPLOAD_DIR / fname

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    return path


# ── modes ────────────────────────────────────────────────────────────────────

def run_backfill():
    """Generate 500 records × last 5 days."""
    today = date.today()
    backfill_dates = [today - timedelta(days=i) for i in range(5, 0, -1)]

    print(f"[BACKFILL] Generating retail data for {len(backfill_dates)} days: "
          f"{backfill_dates[0]} → {backfill_dates[-1]}")
    print(f"           {RECORDS_PER_DAY} records/day  →  {RECORDS_PER_DAY * len(backfill_dates)} total rows")

    total = 0
    for dt in backfill_dates:
        rows = generate_day_retail(dt)
        path = write_day_csv(dt, rows)
        total += len(rows)
        print(f"  ✔  {dt}  {len(rows):>5} records → {path}")

    print(f"\n[BACKFILL] Done. {total} rows written across {len(backfill_dates)} files.")


def run_daily(target_date: date = None):
    """Generate 500 records for tomorrow (or target_date)."""
    if target_date is None:
        target_date = date.today() + timedelta(days=1)

    print(f"[DAILY] Generating {RECORDS_PER_DAY} retail records for {target_date}")

    rows = generate_day_retail(target_date)
    path = write_day_csv(target_date, rows)

    print(f"  ✔  {target_date}  {len(rows)} records → {path}")
    print(f"\n[DAILY] Done. Run your FileExtractor to ingest into the raw layer.")


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate live synthetic retail data (matches UCI Online Retail schema)"
    )
    parser.add_argument(
        "--mode",
        choices=["backfill", "daily"],
        required=True,
        help="backfill = last 5 days  |  daily = tomorrow (or --date)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Override target date for daily mode (YYYY-MM-DD). Default = tomorrow.",
    )
    args = parser.parse_args()

    if args.mode == "backfill":
        run_backfill()
    elif args.mode == "daily":
        target = date.fromisoformat(args.date) if args.date else None
        run_daily(target)


if __name__ == "__main__":
    main()
