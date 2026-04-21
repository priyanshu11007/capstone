"""
scripts/repair_raw_parquet.py

ONE-TIME repair script.

WHY THIS EXISTS
---------------
The raw retail Parquet files were written by an earlier version of
file_extractor.py that used pandas default type inference.  That caused:

  Quantity    → int64  (Parquet INT64)   ← Spark IntegerType expects INT32
  InvoiceDate → datetime64[us]           ← schema expects string then parses

Spark 3.4+ refuses to read INT64 into an IntegerType column (no implicit
downcast), so sales_bronze.py crashes before it can even apply its schema.

WHAT IT DOES
------------
1. Finds every .parquet file under data_lake/raw/retail_sales/
2. Reads it with pandas
3. Casts Quantity  → int32   (maps to Parquet INT32 = Spark IntegerType)
4. Casts InvoiceDate → str   (bronze transformer parses it with to_timestamp)
5. Rewrites the file in-place

After running this script once, all future files written by the fixed
file_extractor.py will already have the right types (it enforces them on
read and uses dtype=str on CSV load), so this script never needs to run again.

USAGE
-----
    cd capstone_fixed/
    python scripts/repair_raw_parquet.py

    # Dry-run (shows what would change, writes nothing):
    python scripts/repair_raw_parquet.py --dry-run
"""

import argparse
import glob
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# Target schema: what every raw retail Parquet file MUST look like so that
# Spark's RAW_RETAIL_SCHEMA in sales_bronze.py can read it without conflict.
# ---------------------------------------------------------------------------
TARGET_DTYPES = {
    "InvoiceNo":   "str",        # was str  ✔ — keep
    "StockCode":   "str",        # was str  ✔ — keep
    "Description": "str",        # was str  ✔ — keep
    "Quantity":    "int32",      # was int64 ✗ → fix to int32 (Parquet INT32)
    "InvoiceDate": "str",        # was datetime64 ✗ → fix to str
    "UnitPrice":   "float64",    # was float64 ✔ — keep
    "CustomerID":  "float64",    # was float64 ✔ — keep
    "Country":     "str",        # was str  ✔ — keep
    # metadata columns (present in most files)
    "_source_file": "str",
    "_entity":      "str",
    "_ingested_at": "str",
}


def repair_file(path: Path, dry_run: bool) -> dict:
    """
    Read, fix, and overwrite a single Parquet file.
    Returns a dict describing what changed.
    """
    df = pd.read_parquet(path)
    changes = []

    for col, target in TARGET_DTYPES.items():
        if col not in df.columns:
            continue

        current = str(df[col].dtype)

        if target == "str":
            if current not in ("object", "string"):
                # datetime → string: format to match UCI pattern
                if "datetime" in current:
                    df[col] = df[col].dt.strftime("%-m/%-d/%Y %H:%M")
                else:
                    df[col] = df[col].astype(str)
                changes.append(f"{col}: {current} → str")

        elif target == "int32":
            if current != "int32":
                df[col] = df[col].astype("int32")
                changes.append(f"{col}: {current} → int32")

        elif target == "float64":
            if current != "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
                changes.append(f"{col}: {current} → float64")

    result = {
        "file": str(path),
        "changes": changes,
        "rewritten": False,
    }

    if changes and not dry_run:
        # Write back with pyarrow, preserving the file path exactly
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, path, compression="snappy")
        result["rewritten"] = True

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Repair raw retail Parquet files: fix Quantity INT64→INT32 "
                    "and InvoiceDate timestamp→string."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be changed without writing anything."
    )
    parser.add_argument(
        "--root", default="data_lake/raw/retail_sales",
        help="Root directory to scan (default: data_lake/raw/retail_sales)"
    )
    args = parser.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"ERROR: directory not found: {root}")
        print("Run this script from your capstone project root (where main.py lives).")
        sys.exit(1)

    files = sorted(root.rglob("*.parquet"))
    if not files:
        print(f"No .parquet files found under {root}")
        sys.exit(0)

    print(f"{'[DRY-RUN] ' if args.dry_run else ''}Scanning {len(files)} file(s) under {root}/\n")

    total_fixed = 0
    for path in files:
        result = repair_file(path, dry_run=args.dry_run)
        # walk_up= was added in Python 3.12; use a fallback for 3.9–3.11
        try:
            rel = Path(result["file"]).relative_to(Path("."))
        except ValueError:
            rel = Path(result["file"])

        if result["changes"]:
            status = "WOULD FIX" if args.dry_run else "FIXED"
            print(f"  [{status}] {path.name}")
            for c in result["changes"]:
                print(f"           {c}")
            total_fixed += 1
        else:
            print(f"  [OK]     {path.name}  (no changes needed)")

    print(f"\n{'[DRY-RUN] ' if args.dry_run else ''}Done.")
    if args.dry_run:
        print(f"  {total_fixed} file(s) would be rewritten. Run without --dry-run to apply.")
    else:
        print(f"  {total_fixed} file(s) rewritten.")
        if total_fixed > 0:
            print("\nNext steps:")
            print("  python main.py --entity sales_bronze")
            print("  python main.py --entity sales_silver")
            print("  python main.py --entity gold")
            print("  python main.py --live backfill")


if __name__ == "__main__":
    main()
