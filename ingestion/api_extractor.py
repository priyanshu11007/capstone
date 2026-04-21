"""
ingestion/api_extractor.py

Config-driven REST API extractor with:
  - Retry logic (exponential back-off)
  - Pagination support
  - Error handling & structured logging
  - Output to raw layer as JSON
"""
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.base import BaseExtractor


class APIExtractor(BaseExtractor):
    """Generic REST API extractor."""

    def __init__(self, config_path: str = "configs/api_config.yaml"):
        super().__init__(config_path, layer="raw")
        self._session = self._build_session()

    # ------------------------------------------------------------------
    # Session with retry
    # ------------------------------------------------------------------
    def _build_session(self) -> requests.Session:
        retry_cfg = (
            self.config.get("apis", {})
            .get("openweathermap", {})
            .get("retry", {})
        )
        retry = Retry(
            total=retry_cfg.get("max_attempts", 3),
            backoff_factor=retry_cfg.get("backoff_factor", 2),
            status_forcelist=retry_cfg.get("status_forcelist", [429, 500, 502, 503]),
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    # ------------------------------------------------------------------
    # Core fetch
    # ------------------------------------------------------------------
    def _get(self, url: str, params: Dict[str, Any]) -> Dict:
        timeout = (
            self.config.get("apis", {})
            .get("openweathermap", {})
            .get("timeout", 30)
        )
        self.logger.info(f"GET {url} params={list(params.keys())}")
        resp = self._session.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def _get_paginated(
        self,
        url: str,
        params: Dict[str, Any],
        page_key: str = "page",
        data_key: str = "results",
        max_pages: int = 100,
    ) -> List[Dict]:
        """Generic paginated fetch (for APIs that support page-based pagination)."""
        all_results: List[Dict] = []
        page = 1
        while page <= max_pages:
            params[page_key] = page
            data = self._get(url, params)
            results = data.get(data_key, [])
            if not results:
                break
            all_results.extend(results)
            self.logger.info(f"  page {page}: {len(results)} records fetched")
            page += 1
            time.sleep(0.5)  # polite delay
        return all_results

    # ------------------------------------------------------------------
    # Weather extraction (Open-Meteo – no API key required)
    # ------------------------------------------------------------------
    def extract_weather(self) -> List[Path]:
        """
        Pull daily historical weather for all configured cities.
        Uses Open-Meteo archive API (free, no key needed).
        Returns list of written file paths.
        """
        ingestion_cfg = self.config.get("ingestion", {}).get("weather", {})
        cities = self.config.get("cities", [])
        base_url = ingestion_cfg.get("base_url")
        start_date = ingestion_cfg.get("date_range", {}).get("start")
        end_date = ingestion_cfg.get("date_range", {}).get("end")
        variables = ingestion_cfg.get("variables", [])
        timezone = ingestion_cfg.get("timezone", "Europe/London")

        written: List[Path] = []

        for city in cities:
            city_name = city["name"]
            self.logger.info(f"Extracting weather for {city_name} [{start_date} → {end_date}]")

            params = {
                "latitude": city["lat"],
                "longitude": city["lon"],
                "start_date": start_date,
                "end_date": end_date,
                "daily": ",".join(variables),
                "timezone": timezone,
            }

            try:
                data = self._get(base_url, params)

                if not self.validate(data):
                    self.logger.warning(f"Validation failed for {city_name}, skipping.")
                    continue

                # Enrich with city metadata
                data["_meta"] = {
                    "city": city_name,
                    "country": city.get("country"),
                    "lat": city["lat"],
                    "lon": city["lon"],
                    "extracted_at": __import__("datetime").datetime.utcnow().isoformat(),
                    "source": "open_meteo_archive",
                }

                # Generic partition: entity/source/city=X/year=YYYY
                # We store one file per city covering the full range
                out_dir = self._build_output_path(
                    entity="weather",
                    source_name="open_meteo",
                    partition={"city": city_name},
                )
                fname = self._timestamped_filename(
                    prefix=f"weather_{city_name.lower()}_{start_date}_{end_date}",
                    extension="json",
                )
                out_path = out_dir / fname
                with open(out_path, "w") as f:
                    json.dump(data, f, indent=2)

                self.logger.info(f"  ✔ Written → {out_path}")
                written.append(out_path)

            except requests.HTTPError as e:
                self.logger.error(f"HTTP error for {city_name}: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error for {city_name}: {e}", exc_info=True)

        return written

    # ------------------------------------------------------------------
    # Generic extract dispatcher
    # ------------------------------------------------------------------
    def extract(self, entity: str = "weather", **kwargs) -> List[Path]:
        dispatch = {
            "weather": self.extract_weather,
        }
        fn = dispatch.get(entity)
        if fn is None:
            raise ValueError(f"Unknown entity '{entity}'. Available: {list(dispatch)}")
        return fn(**kwargs)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def validate(self, data: Any) -> bool:
        if not isinstance(data, dict):
            self.logger.error("Response is not a JSON object.")
            return False
        if "daily" not in data:
            self.logger.error("Response missing 'daily' key.")
            return False
        daily = data["daily"]
        if "time" not in daily or not daily["time"]:
            self.logger.error("'daily.time' is empty.")
            return False
        self.logger.info(f"  Validated: {len(daily['time'])} daily records.")
        return True
