"""
ingestion/file_extractor.py

Handles ingestion of manually uploaded flat files (CSV, Parquet).

Design decision:
  - Manual uploads must be placed in:  manual-uploads/<entity>/
  - They are NEVER dropped directly into data_lake/raw/
  - This extractor copies & standardises them into the raw layer
    under the generic partition structure.
"""
import shutil
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd

from ingestion.base import BaseExtractor


class FileExtractor(BaseExtractor):
    """Ingests CSV / Parquet files from manual-uploads into raw layer."""

    MANUAL_UPLOADS_ROOT = Path("manual-uploads")

    def __init__(self, config_path: str = "configs/api_config.yaml"):
        super().__init__(config_path, layer="raw")

    # ------------------------------------------------------------------
    # Core
    # ------------------------------------------------------------------
    def extract(
        self,
        entity: str,
        source_name: str,
        file_name: Optional[str] = None,
        output_format: str = "parquet",
        **kwargs,
    ) -> List[Path]:
        """
        entity      : logical name, e.g. 'retail_sales'
        source_name : sub-folder in manual-uploads, e.g. 'uci_online_retail'
        file_name   : specific file; if None, all files in source_name dir
        output_format: 'parquet' (default) or 'csv'
        """
        source_dir = self.MANUAL_UPLOADS_ROOT / entity / source_name
        if not source_dir.exists():
            raise FileNotFoundError(
                f"Manual-uploads directory not found: {source_dir}\n"
                f"Place your files under: {source_dir}/"
            )

        files = (
            [source_dir / file_name]
            if file_name
            else list(source_dir.glob("*.csv")) + list(source_dir.glob("*.parquet"))
        )

        if not files:
            self.logger.warning(f"No files found in {source_dir}")
            return []

        written: List[Path] = []
        for fp in files:
            self.logger.info(f"Processing file: {fp}")
            df = self._read_file(fp)

            if not self.validate(df):
                self.logger.warning(f"Skipping {fp.name} – failed validation.")
                continue

            df = self._add_metadata(df, source_file=fp.name, entity=entity)

            # Partition by year/month if 'InvoiceDate' column exists
            partitions = self._detect_partitions(df)

            if partitions:
                for partition_vals, subset in partitions:
                    out_dir = self._build_output_path(
                        entity=entity,
                        source_name=source_name,
                        partition=partition_vals,
                    )
                    out_path = self._write(subset, out_dir, fp.stem, output_format)
                    written.append(out_path)
            else:
                out_dir = self._build_output_path(entity=entity, source_name=source_name)
                out_path = self._write(df, out_dir, fp.stem, output_format)
                written.append(out_path)

        self.logger.info(f"File extraction complete. {len(written)} file(s) written.")
        return written

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _read_file(self, fp: Path) -> pd.DataFrame:
        ext = fp.suffix.lower()
        if ext == ".csv":
            # Try common encodings for UCI dataset
            for enc in ["utf-8", "latin-1", "cp1252"]:
                try:
                    return pd.read_csv(fp, encoding=enc)
                except UnicodeDecodeError:
                    continue
            raise ValueError(f"Could not decode {fp} with any known encoding.")
        elif ext == ".parquet":
            return pd.read_parquet(fp)
        else:
            raise ValueError(f"Unsupported file type: {ext}")

    def _add_metadata(self, df: pd.DataFrame, source_file: str, entity: str) -> pd.DataFrame:
        df = df.copy()
        df["_source_file"] = source_file
        df["_entity"] = entity
        df["_ingested_at"] = pd.Timestamp.utcnow().isoformat()
        return df

    def _detect_partitions(self, df: pd.DataFrame):
        """
        If the dataframe has an InvoiceDate column, yield (partition_dict, subset_df)
        for each year-month combination.
        """
        date_col = None
        for candidate in ["InvoiceDate", "invoice_date", "date", "Date"]:
            if candidate in df.columns:
                date_col = candidate
                break

        if date_col is None:
            return None

        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df["_year"] = df[date_col].dt.year.astype(str)
        df["_month"] = df[date_col].dt.month.astype(str).str.zfill(2)

        groups = []
        for (year, month), subset in df.groupby(["_year", "_month"]):
            subset = subset.drop(columns=["_year", "_month"])
            groups.append(({"year": year, "month": month}, subset))
        return groups

    def _write(
        self, df: pd.DataFrame, out_dir: Path, stem: str, fmt: str
    ) -> Path:
        fname = self._timestamped_filename(prefix=stem, extension=fmt)
        out_path = out_dir / fname
        if fmt == "parquet":
            df.to_parquet(out_path, index=False)
        else:
            df.to_csv(out_path, index=False)
        self.logger.info(f"  ✔ Written → {out_path}  ({len(df)} rows)")
        return out_path

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def validate(self, data: Any) -> bool:
        if not isinstance(data, pd.DataFrame):
            self.logger.error("Data is not a DataFrame.")
            return False
        if data.empty:
            self.logger.error("DataFrame is empty.")
            return False
        self.logger.info(f"  Validated: {len(data)} rows, {len(data.columns)} columns.")
        return True
