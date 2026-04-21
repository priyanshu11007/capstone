"""
tests/test_ingestion.py

Unit tests for ingestion/base.py, ingestion/api_extractor.py,
and ingestion/file_extractor.py.

Run:
    pytest tests/test_ingestion.py -v
    pytest tests/test_ingestion.py -v --tb=short   # compact tracebacks
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pandas as pd
import pytest

# ── bootstrap project root ───────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.base import BaseExtractor
from ingestion.api_extractor import APIExtractor
from ingestion.file_extractor import FileExtractor, UCI_RETAIL_DTYPES


# ══════════════════════════════════════════════════════════════════════════════
# Helpers / stubs
# ══════════════════════════════════════════════════════════════════════════════

MINIMAL_CONFIG = {
    "apis": {
        "openweathermap": {
            "retry": {"max_attempts": 2, "backoff_factor": 1,
                      "status_forcelist": [429, 500]},
            "timeout": 5,
        }
    },
    "cities": [
        {"name": "London",     "country": "GB", "lat": 51.5074, "lon": -0.1278},
        {"name": "Manchester", "country": "GB", "lat": 53.4808, "lon": -2.2426},
    ],
    "ingestion": {
        "weather": {
            "source": "open_meteo",
            "base_url": "https://archive-api.open-meteo.com/v1/archive",
            "date_range": {"start": "2022-01-01", "end": "2022-01-03"},
            "variables": ["temperature_2m_max", "temperature_2m_min",
                          "precipitation_sum"],
            "timezone": "Europe/London",
            "output_format": "json",
        }
    },
}

VALID_WEATHER_RESPONSE = {
    "daily": {
        "time":                ["2022-01-01", "2022-01-02", "2022-01-03"],
        "temperature_2m_max":  [5.0, 6.0, 4.0],
        "temperature_2m_min":  [1.0, 2.0, 0.0],
        "precipitation_sum":   [0.0, 2.5, 0.0],
    },
    "latitude": 51.5,
    "longitude": -0.12,
}

# ── Minimal concrete subclass for BaseExtractor tests ───────────────────────

class _ConcreteExtractor(BaseExtractor):
    def extract(self, **kwargs):
        return "extracted"
    def validate(self, data):
        return True


# ══════════════════════════════════════════════════════════════════════════════
# 1. BaseExtractor
# ══════════════════════════════════════════════════════════════════════════════

class TestBaseExtractor(unittest.TestCase):

    def test_instantiation_no_config(self):
        """BaseExtractor can be created without a config file."""
        ext = _ConcreteExtractor(config_path=None, layer="raw")
        self.assertEqual(ext.layer, "raw")
        self.assertEqual(ext.config, {})

    def test_load_config_from_yaml(self):
        """_load_config parses YAML and returns a dict."""
        yaml_content = "key: value\nnested:\n  a: 1\n"
        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w",
                                         delete=False) as f:
            f.write(yaml_content)
            tmp = f.name
        ext = _ConcreteExtractor.__new__(_ConcreteExtractor)
        _ConcreteExtractor.__init__(ext, config_path=tmp, layer="raw")
        self.assertEqual(ext.config["key"], "value")
        self.assertEqual(ext.config["nested"]["a"], 1)

    def test_resolve_env_var_found(self):
        """${VAR} placeholders are replaced with environment values."""
        import os
        os.environ["TEST_DQ_VAR"] = "hello"
        ext = _ConcreteExtractor(config_path=None)
        result = ext._resolve_env_vars({"k": "${TEST_DQ_VAR}"})
        self.assertEqual(result["k"], "hello")

    def test_resolve_env_var_missing_returns_none(self):
        """Missing env var returns None (with a warning)."""
        ext = _ConcreteExtractor(config_path=None)
        result = ext._resolve_env_vars("${DEFINITELY_NOT_SET_XYZZY}")
        self.assertIsNone(result)

    def test_build_output_path_creates_directory(self):
        """_build_output_path creates the directory and returns a Path."""
        with tempfile.TemporaryDirectory() as tmp:
            ext = _ConcreteExtractor(config_path=None, layer="raw")
            # Temporarily patch the working directory
            import os
            orig = os.getcwd()
            try:
                os.chdir(tmp)
                p = ext._build_output_path("weather", "open_meteo",
                                             partition={"city": "London"})
                self.assertTrue(p.exists())
                self.assertTrue(str(p).endswith("city=London"))
            finally:
                os.chdir(orig)

    def test_timestamped_filename_format(self):
        """Timestamped filename matches expected pattern."""
        import re
        ext = _ConcreteExtractor(config_path=None)
        fname = ext._timestamped_filename("weather_london", "json")
        self.assertRegex(fname, r"^weather_london_\d{8}T\d{6}Z\.json$")

    def test_logger_not_none(self):
        ext = _ConcreteExtractor(config_path=None)
        self.assertIsNotNone(ext.logger)


# ══════════════════════════════════════════════════════════════════════════════
# 2. APIExtractor
# ══════════════════════════════════════════════════════════════════════════════

class TestAPIExtractor(unittest.TestCase):

    def _make_extractor(self) -> APIExtractor:
        """Return an APIExtractor with MINIMAL_CONFIG (no real YAML file needed)."""
        ext = APIExtractor.__new__(APIExtractor)
        # Call BaseExtractor.__init__ manually to avoid file I/O
        from ingestion.base import BaseExtractor as BE
        BE.__init__(ext, config_path=None, layer="raw")
        ext.config = MINIMAL_CONFIG
        ext._session = ext._build_session()
        return ext

    # ── validate() ──────────────────────────────────────────────────────────

    def test_validate_returns_true_for_valid_response(self):
        ext = self._make_extractor()
        self.assertTrue(ext.validate(VALID_WEATHER_RESPONSE))

    def test_validate_fails_when_not_dict(self):
        ext = self._make_extractor()
        self.assertFalse(ext.validate([1, 2, 3]))

    def test_validate_fails_when_no_daily_key(self):
        ext = self._make_extractor()
        self.assertFalse(ext.validate({"latitude": 51.5}))

    def test_validate_fails_when_daily_time_empty(self):
        ext = self._make_extractor()
        bad = {"daily": {"time": []}}
        self.assertFalse(ext.validate(bad))

    # ── extract_weather() (mocked HTTP) ─────────────────────────────────────

    @patch("ingestion.api_extractor.APIExtractor._get")
    def test_extract_weather_calls_api_per_city(self, mock_get):
        """extract_weather calls _get once per configured city."""
        mock_get.return_value = VALID_WEATHER_RESPONSE
        ext = self._make_extractor()

        with tempfile.TemporaryDirectory() as tmp:
            import os; orig = os.getcwd(); os.chdir(tmp)
            try:
                written = ext.extract_weather()
            finally:
                os.chdir(orig)

        # 2 cities in MINIMAL_CONFIG → 2 API calls → 2 files
        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(len(written), 2)

    @patch("ingestion.api_extractor.APIExtractor._get")
    def test_extract_weather_writes_valid_json(self, mock_get):
        """Each written file is valid JSON containing the original response."""
        mock_get.return_value = VALID_WEATHER_RESPONSE
        ext = self._make_extractor()

        with tempfile.TemporaryDirectory() as tmp:
            import os; orig = os.getcwd(); os.chdir(tmp)
            try:
                written = ext.extract_weather()
                with open(written[0]) as f:
                    content = json.load(f)
            finally:
                os.chdir(orig)

        self.assertIn("daily", content)
        self.assertIn("_meta", content)
        self.assertEqual(content["_meta"]["city"], "London")

    @patch("ingestion.api_extractor.APIExtractor._get")
    def test_extract_weather_skips_invalid_response(self, mock_get):
        """City with invalid API response is skipped; no file written."""
        mock_get.return_value = {"error": "not found"}   # no 'daily' key
        ext = self._make_extractor()

        with tempfile.TemporaryDirectory() as tmp:
            import os; orig = os.getcwd(); os.chdir(tmp)
            try:
                written = ext.extract_weather()
            finally:
                os.chdir(orig)

        self.assertEqual(len(written), 0)

    @patch("ingestion.api_extractor.APIExtractor._get")
    def test_extract_dispatcher_unknown_entity_raises(self, _):
        """Requesting an unknown entity raises ValueError."""
        ext = self._make_extractor()
        with self.assertRaises(ValueError):
            ext.extract(entity="unicorn")

    @patch("ingestion.api_extractor.APIExtractor._get",
           side_effect=Exception("network error"))
    def test_extract_weather_continues_after_http_error(self, _):
        """HTTP error on one city doesn't crash the whole run."""
        ext = self._make_extractor()
        with tempfile.TemporaryDirectory() as tmp:
            import os; orig = os.getcwd(); os.chdir(tmp)
            try:
                written = ext.extract_weather()   # should not raise
            finally:
                os.chdir(orig)
        self.assertEqual(written, [])


# ══════════════════════════════════════════════════════════════════════════════
# 3. FileExtractor
# ══════════════════════════════════════════════════════════════════════════════

SAMPLE_RETAIL_CSV_ROWS = """InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,2011-01-10 08:26:00,2.55,17850,United Kingdom
536366,22632,HAND WARMER UNION JACK,6,2011-02-15 08:28:00,1.85,17851,United Kingdom
536367,22633,HAND WARMER RED POLKA DOT,6,2011-01-20 08:34:00,1.85,17852,United Kingdom
"""


class TestFileExtractor(unittest.TestCase):

    def _make_extractor(self) -> FileExtractor:
        ext = FileExtractor.__new__(FileExtractor)
        from ingestion.base import BaseExtractor as BE
        BE.__init__(ext, config_path=None, layer="raw")
        ext.config = MINIMAL_CONFIG
        return ext

    # ── validate() ──────────────────────────────────────────────────────────

    def test_validate_ok_dataframe(self):
        ext = self._make_extractor()
        df = pd.DataFrame({"a": [1, 2, 3]})
        self.assertTrue(ext.validate(df))

    def test_validate_fails_on_empty_dataframe(self):
        ext = self._make_extractor()
        self.assertFalse(ext.validate(pd.DataFrame()))

    def test_validate_fails_on_non_dataframe(self):
        ext = self._make_extractor()
        self.assertFalse(ext.validate({"a": 1}))

    # ── _enforce_schema() ────────────────────────────────────────────────────

    

    def test_enforce_schema_skips_unknown_entity(self):
        ext = self._make_extractor()
        df = pd.DataFrame({"x": [1, 2]})
        result = ext._enforce_schema(df, "some_other_entity")
        pd.testing.assert_frame_equal(result, df)

    def test_enforce_schema_handles_missing_columns_gracefully(self):
        """Columns in UCI_RETAIL_DTYPES that aren't in the df are silently skipped."""
        ext = self._make_extractor()
        df = pd.DataFrame({"InvoiceNo": [1, 2], "Quantity": ["3", "4"]})
        result = ext._enforce_schema(df, "retail_sales")
        self.assertIn("InvoiceNo", result.columns)   # still present

    # ── _read_file() ─────────────────────────────────────────────────────────

    def test_read_file_csv(self):
        ext = self._make_extractor()
        with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
            f.write(SAMPLE_RETAIL_CSV_ROWS)
            tmp = f.name
        df = ext._read_file(Path(tmp))
        self.assertEqual(len(df), 3)
        self.assertIn("InvoiceNo", df.columns)



    def test_read_file_unsupported_extension_raises(self):
        ext = self._make_extractor()
        with self.assertRaises(ValueError):
            ext._read_file(Path("some_file.xlsx"))

    # ── _detect_partitions() ─────────────────────────────────────────────────

    def test_detect_partitions_returns_year_month_groups(self):
        ext = self._make_extractor()
        df = pd.read_csv(
            __import__("io").StringIO(SAMPLE_RETAIL_CSV_ROWS),
            dtype=str
        )
        groups = ext._detect_partitions(df)
        self.assertIsNotNone(groups)
        years_months = [(g[0]["year"], g[0]["month"]) for g in groups]
        self.assertIn(("2011", "01"), years_months)
        self.assertIn(("2011", "02"), years_months)

    def test_detect_partitions_returns_none_without_date_col(self):
        ext = self._make_extractor()
        df = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
        result = ext._detect_partitions(df)
        self.assertIsNone(result)

    # ── _add_metadata() ─────────────────────────────────────────────────────

    def test_add_metadata_adds_three_columns(self):
        ext = self._make_extractor()
        df = pd.DataFrame({"val": [1]})
        result = ext._add_metadata(df, "test.csv", "retail_sales")
        self.assertIn("_source_file", result.columns)
        self.assertIn("_entity", result.columns)
        self.assertIn("_ingested_at", result.columns)
        self.assertEqual(result["_source_file"].iloc[0], "test.csv")

    # ── extract() integration (filesystem-backed) ────────────────────────────

    
    def test_extract_raises_if_source_dir_missing(self):
        ext = self._make_extractor()
        with tempfile.TemporaryDirectory() as tmp:
            import os; orig = os.getcwd(); os.chdir(tmp)
            try:
                with self.assertRaises(FileNotFoundError):
                    ext.extract(entity="retail_sales",
                                source_name="nonexistent_source")
            finally:
                os.chdir(orig)


# ══════════════════════════════════════════════════════════════════════════════
# Run directly: python tests/test_ingestion.py
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    unittest.main(verbosity=2)
